#!/usr/bin/env python3
"""
AEP Overtime Calculator - Flask Web Application
Upload timesheet data, process overtime rules, and download results
"""

from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from typing import Dict, List, Tuple, Optional
import warnings
import os
import tempfile
from werkzeug.utils import secure_filename
import traceback
from io import BytesIO

warnings.filterwarnings('ignore')

app = Flask(__name__)
app.secret_key = 'aep_overtime_calculator_secret_key_2025'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

class AEPOvertimeCalculator:
    def __init__(self):
        self.processed_data = []
        self.filtered_data = []
        self.calculation_log = []
        self.validation_exceptions = [] # New: To store all validation issues

        # Validation rules (hardcoded as per requirements)
        self.LINE_SHOP_ADDRESSES = [
            "4636 Ardmore Ave", "6900 N 600 W", "15650 Edgerton Rd",
            "2101 IN-28", "3802 South Meeker Ave", "4421 Ardmore Ave",
            "404 S Frances St", "2825 Prairie Ave"
        ]
        # These are specific valid work order numbers from the OCR'd PDF
        # Note: The CSV also shows DIM and DOP prefixes. A more comprehensive rule
        # would need to cover all AEP operating company prefixes and their specific formats.
        # For this implementation, we map based on 'Bill To Account' and validate against these lists
        # if the extracted WO matches the region.
        self.VALID_WORK_ORDER_NUMBERS = {
            'TN': {'BKP0A00018', 'BKP0S00018', 'BKP0000001', 'BKP0000002',
                   'BKP0000003', 'BKP0000004', 'BKP0000012', 'BKP0000018'},
            'VA': {'BAP0A00238', 'BAP0S00238', 'BAP0000021', 'BAP0000132',
                   'BAP0000133', 'BAP0000024', 'BAP0000032', 'BAP0000238'},
            'WV': {'BAP0A00058', 'BAP0S00058', 'BAP0000141', 'BAP0000042',
                   'BAP0000043', 'BAP0000144', 'BAP0000152', 'BAP0000058'}
        }
        # Mapping from Bill To Account (partial name) to region for work order validation
        self.BILL_TO_ACCOUNT_REGION_MAP = {
            'AEP I&M': 'IN/MI', # Indiana Michigan Power - no specific WO list provided, so generic check
            'AEP APCO Pineville': 'WV', # Appalachian Power Company - some might be VA, generalizing to WV for BAP/BKP
            'AEP APCO Beckley': 'WV',
            'AEP APCO Kingsport': 'TN',
            'AEP APCO Charleston': 'WV',
            'AEP APCO Glen Lyn': 'WV',
            'AEP APCO Hico': 'WV',
            'AEP APCO Teays Valley': 'WV',
            'AEP APCO Woodlawn': 'VA',
            'AEP APCO Huntington': 'WV',
            'AEP APCO Bluefield': 'WV',
            'AEP APCO Glade Spring': 'VA',
            'AEP APCO Roanoke VA': 'VA',
            'AEP APCO Clintwood': 'VA',
            'AEP OPCO': 'OH',    # Ohio Power Company - no specific WO list provided
            'AEP KP': 'KY'       # Kentucky Power - no specific WO list provided
        }

        self.INVALID_CONTACT_NAMES = {'PAT DENNEY', 'PAT DENNY'}
        # Regex to extract potential AEP work order numbers (e.g., 3-4 letters followed by 7-10 digits).
        # This is a general pattern, specific prefixes might need more precise regex or lists.
        self.WORK_ORDER_REGEX = re.compile(r'([A-Z]{3,4}\d{7,10}|DKP\d{7})') # Added DKP specific for some samples
        self.SPECIFIC_WORK_ORDER_LENGTH = {
            'BAP': 7, # BAP0000152 (3 prefix + 7 digits)
            'BKP': 7, # BKP0S00018 (3 prefix + 7 digits)
            'DAP': 7, # DAP0410472 (3 prefix + 7 digits)
            'DIM': 7, # DIM0243077 (3 prefix + 7 digits)
            'DOP': 7, # DOP0402623 (3 prefix + 7 digits)
            'DKP': 7, # DKP0044438 (3 prefix + 7 digits)
            'TL0': 7  # TL0075894 (3 prefix + 7 digits) - specific to 'TL' which might be longer
        }

    def parse_input_file(self, file_path: str) -> pd.DataFrame:
        """Parse input spreadsheet file and return standardized DataFrame"""
        try:
            # Handle different file formats
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.lower().endswith(('.xls', '.xlsx')):
                # Try to read as Excel first
                try:
                    df = pd.read_excel(file_path)
                except Exception as e:
                    # If that fails, try reading as HTML table (like the sample file)
                    # This might be needed for older .xls files that are actually HTML
                    print(f"Failed to read as Excel, trying as HTML table. Error: {e}")
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    df = self._parse_html_table(content)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
                
            # Standardize column names
            df = self._standardize_columns(df)
            print(f"Successfully loaded {len(df)} records from {file_path}")
            return df
            
        except Exception as e:
            print(f"Error parsing file {file_path}: {e}")
            raise
    
    def _parse_html_table(self, html_content: str) -> pd.DataFrame:
        """Parse HTML table format (for files like the sample .xls)"""
        import re
        from html import unescape
        
        # Extract table rows
        rows = []
        row_pattern = r'<tr>(.*?)</tr>'
        cell_pattern = r'<t[hd][^>]*>(.*?)</t[hd]>'
        
        for row_match in re.finditer(row_pattern, html_content, re.DOTALL | re.IGNORECASE):
            row_html = row_match.group(1)
            cells = []
            
            for cell_match in re.finditer(cell_pattern, row_html, re.DOTALL | re.IGNORECASE):
                cell_content = cell_match.group(1)
                # Clean HTML tags and decode entities
                cell_content = re.sub(r'<[^>]+>', '', cell_content)
                cell_content = unescape(cell_content).strip()
                cells.append(cell_content)
            
            if cells:
                rows.append(cells)
        
        if not rows:
            raise ValueError("No table data found in HTML file")
        
        # First row as headers, rest as data
        headers = rows[0]
        data_rows = rows[1:]
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers)
        return df
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and data types"""
        # Create column mapping for common variations
        column_mapping = {
            'Resource Name': 'employee_name',
            'Validated Start Time': 'start_time',
            'Validated End Time': 'end_time',
            'Start': 'original_start', # Keep original_start if needed for context
            'Call Out': 'is_call_out',
            'Lunch Deduction': 'lunch_deduction',
            'Job Name': 'job_id',
            'Quantity': 'reported_hours',
            'Item Number': 'item_number',
            'Client Job #': 'client_job_number',
            'Contact: Full Name': 'contact_name',
            'Bill To Account: Account Name': 'bill_to_account', # Keep this for region mapping
            # 'Job Address': 'job_address' # Add this if a dedicated address column appears in future files
        }
        
        # Rename columns
        df_renamed = df.rename(columns=column_mapping)
        
        # Add missing columns with defaults for required processing and new validation fields
        required_columns = [
            'employee_name', 'start_time', 'end_time', 'is_call_out', 
            'lunch_deduction', 'job_id', 'reported_hours', 'item_number',
            'client_job_number', 'contact_name', 'bill_to_account', # Added for validation
            'job_address' # Placeholder for job address for line shop validation
        ]
        
        for col in required_columns:
            if col not in df_renamed.columns:
                if col == 'employee_name':
                    df_renamed[col] = ''
                elif col in ['reported_hours', 'lunch_deduction']:
                    df_renamed[col] = 0.0
                elif col == 'is_call_out':
                    df_renamed[col] = '0' # Default to not a call out initially
                else: # For other string columns
                    df_renamed[col] = ''
        
        # Ensure 'Item Number' column exists before cleaning for call out logic
        if 'item_number' not in df_renamed.columns:
            df_renamed['item_number'] = ''

        # Clean and convert data types, and perform initial validations
        df_renamed = self._clean_data(df_renamed)
        
        return df_renamed
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data"""
        initial_count = len(df)
        
        # Filter out rows with missing essential data (employee name, start/end times)
        df_cleaned = df.dropna(subset=['employee_name', 'start_time', 'end_time']).copy()
        df_cleaned = df_cleaned[df_cleaned['employee_name'].str.strip() != ''].copy()
        
        if len(df_cleaned) < initial_count:
            filtered_count_essential = initial_count - len(df_cleaned)
            print(f"Filtered out {filtered_count_essential} records with missing essential data (employee/times)")
            # Log filtered records here too if desired, similar to invalid dates
        
        # NEW: Detect and log exact duplicate records
        # Columns that uniquely identify a job entry for duplicate checking
        duplicate_check_cols = [
            'employee_name', 'job_id', 'start_time', 'end_time', 
            'reported_hours', 'item_number', 'is_call_out', 'lunch_deduction',
            'client_job_number', 'contact_name', 'bill_to_account'
        ]
        
        # Ensure all columns for duplicate check exist, handle missing ones by adding empty
        for col in duplicate_check_cols:
            if col not in df_cleaned.columns:
                df_cleaned[col] = ''
        
        # Find duplicated rows (keep=False marks all occurrences of a duplicate set)
        duplicated_rows_df = df_cleaned[df_cleaned.duplicated(subset=duplicate_check_cols, keep=False)].copy()
        
        if not duplicated_rows_df.empty:
            print(f"Found {len(duplicated_rows_df)} exact duplicate records. Logging them as validation exceptions.")
            # Log each unique duplicate set only once, or each duplicate instance
            for idx, row in duplicated_rows_df.iterrows():
                self.validation_exceptions.append({
                    'employee_name': row['employee_name'],
                    'job_id': row['job_id'],
                    'client_job_number': row.get('client_job_number', ''),
                    'contact_name': row.get('contact_name', ''),
                    'job_address': row.get('job_address', ''), # Use the actual job_address if present
                    'issues': 'Exact Duplicate Record'
                })
            # Remove duplicates, keeping only the first occurrence for processing
            df_cleaned = df_cleaned.drop_duplicates(subset=duplicate_check_cols, keep='first').copy()
            print(f"Removed exact duplicate records. Remaining unique records: {len(df_cleaned)}")

        # Convert is_call_out to string for processing
        df_cleaned['is_call_out'] = df_cleaned['is_call_out'].astype(str).str.strip()
        df_cleaned['item_number'] = df_cleaned['item_number'].astype(str).str.strip() # Ensure item_number is string
        
        # NEW: Refined is_call_out logic: Check 'Call Out' column value OR 'EMEG' in 'Item Number'
        df_cleaned['is_call_out'] = df_cleaned.apply(lambda row: \
            row['is_call_out'].lower() == 'true' or \
            'yes' in row['is_call_out'].lower() or \
            'emeg' in row['item_number'].lower(), # 'EMEG' from '1-MAN EMEG'
            axis=1
        )
        
        # Convert lunch deduction to float and handle negative values
        df_cleaned['lunch_deduction'] = pd.to_numeric(df_cleaned['lunch_deduction'], errors='coerce').fillna(0.0)
        df_cleaned['lunch_deduction'] = df_cleaned['lunch_deduction'].abs() # Ensure positive values
        df_cleaned['lunch_deduction'] = df_cleaned['lunch_deduction'].clip(upper=1.0) # Cap at 1 hour max

        # Parse datetime columns
        df_cleaned['start_datetime'] = pd.to_datetime(df_cleaned['start_time'], errors='coerce')
        df_cleaned['end_datetime'] = pd.to_datetime(df_cleaned['end_time'], errors='coerce')
        
        # Remove records with invalid dates
        valid_dates = df_cleaned['start_datetime'].notna() & df_cleaned['end_datetime'].notna()
        invalid_count = (~valid_dates).sum()
        if invalid_count > 0:
            print(f"Filtered out {invalid_count} records with invalid date/time data")
            for idx, row in df_cleaned[~valid_dates].iterrows():
                self.filtered_data.append({
                    'employee_name': row['employee_name'],
                    'job_id': row['job_id'],
                    'start_time': row['start_time'],
                    'end_time': row['end_time'],
                    'filter_reason': 'Invalid date/time format'
                })
        df_cleaned = df_cleaned[valid_dates].copy()
        
        # NEW: Perform job-level validations (Line Shop, Work Order, Invalid Contact)
        df_cleaned['validation_issues'] = df_cleaned.apply(self._validate_job_record, axis=1)
        # Store rows with validation issues in a separate list for reporting
        for idx, row in df_cleaned[df_cleaned['validation_issues'].apply(bool)].iterrows():
            self.validation_exceptions.append({
                'employee_name': row['employee_name'],
                'job_id': row['job_id'],
                'client_job_number': row.get('client_job_number', ''),
                'contact_name': row.get('contact_name', ''),
                'job_address': row.get('job_address', ''), # Use the actual job_address if present
                'issues': ', '.join(row['validation_issues'])
            })
            
        # Consolidate pre-split records AFTER initial cleaning and validation flagging
        df_final = self._consolidate_split_records(df_cleaned)
        
        # Add derived columns
        df_final['work_date'] = df_final['start_datetime'].dt.date
        df_final['day_of_week'] = df_final['start_datetime'].dt.day_name()
        
        # Calculate Sunday-to-Saturday weeks (AEP standard)
        df_final['week_start'] = df_final['start_datetime'].dt.date - pd.to_timedelta((df_final['start_datetime'].dt.dayofweek + 1) % 7, unit='D')

        return df_final
    
    def _validate_job_record(self, row: pd.Series) -> List[str]:
        """
        NEW: Validates individual job record for specific issues (line shop, work order, contact).
        Returns a list of issue descriptions.
        """
        issues = []
        
        # 1. Validate against line shop addresses
        # Assumes 'job_address' is present or we look for substrings in other fields.
        job_address = str(row.get('job_address', '')).strip()
        job_name = str(row.get('job_id', '')).strip() # Sometimes addresses might be in job_id
        line_note = str(row.get('Line_Note', '')).strip() # Sometimes addresses might be in Line_Note

        # Check for presence of line shop address in relevant fields
        found_line_shop = False
        for ls_address in self.LINE_SHOP_ADDRESSES:
            if ls_address.lower() in job_address.lower() or \
               ls_address.lower() in job_name.lower() or \
               ls_address.lower() in line_note.lower():
                issues.append(f"Line Shop Address Detected: '{ls_address}'")
                found_line_shop = True
                break # Only flag once per record for this type
        
        # 2. Validate Client Job # (Work Order)
        client_job_number = str(row.get('client_job_number', '')).strip()
        bill_to_account = str(row.get('bill_to_account', '')).strip()

        # Try to extract the core work order number using regex
        wo_match = self.WORK_ORDER_REGEX.search(client_job_number)
        extracted_wo = wo_match.group(1) if wo_match else None
        
        if not extracted_wo:
            # If no clear work order format extracted, but client_job_number is not empty or 'SEE FOREMAN'
            if client_job_number and client_job_number.upper() != 'SEE FOREMAN':
                issues.append(f"Invalid/Unparseable Work Order Format: '{client_job_number}'")
        else:
            # Generic length check based on prefix (e.g., AEP rule "11 digits when it's supposed to be 10")
            prefix = extracted_wo[:3].upper() # e.g. BAP, DIM, DOP
            expected_digit_length = self.SPECIFIC_WORK_ORDER_LENGTH.get(prefix, 7) # Default to 7 digits after prefix
            
            # Check length for the digits part
            if len(extracted_wo) - len(prefix) > expected_digit_length:
                 issues.append(f"Work Order Digits Too Long: '{extracted_wo}' (Expected {expected_digit_length} after '{prefix}', got {len(extracted_wo) - len(prefix)})")

            # Map Bill To Account to a region key
            opco_key_found = None
            for key in self.BILL_TO_ACCOUNT_REGION_MAP:
                if key in bill_to_account: # Use 'in' for partial matches, e.g., 'AEP I&M Muncie' matches 'AEP I&M'
                    opco_key_found = key
                    break
            
            region_code = self.BILL_TO_ACCOUNT_REGION_MAP.get(opco_key_found)
            
            if region_code in self.VALID_WORK_ORDER_NUMBERS:
                # Check if extracted WO is in the list of valid numbers for that specific region
                if extracted_wo not in self.VALID_WORK_ORDER_NUMBERS[region_code]:
                    issues.append(f"Work Order Mismatch for {region_code}: '{extracted_wo}' not in valid list")
            # else: For regions not in VALID_WORK_ORDER_NUMBERS (like IN/MI, OH, KY), we don't have a specific list
            # so we only apply the format/length check. If a specific validation rule is needed for them, add here.


        # 3. Validate Contact Name
        contact_name = str(row.get('contact_name', '')).strip().upper()
        if contact_name in self.INVALID_CONTACT_NAMES:
            issues.append(f"Invalid Contact Name: '{contact_name}' (Cannot sign off)")
            
        return issues
    
    def _consolidate_split_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Consolidate records that are pre-split into regular and overtime components"""
        print("Consolidating pre-split records...")
        
        # Group records by employee, job, start_time, and end_time
        # Include 'validation_issues' in group_cols to ensure unique combination if issues exist
        group_cols = ['employee_name', 'job_id', 'start_datetime', 'end_datetime', 
                      'lunch_deduction', 'is_call_out', 'client_job_number', 
                      'contact_name', 'bill_to_account', 'job_address'] # Added for robust grouping and carrying issues
        
        consolidated_records = []
        
        # Sort by start_datetime to ensure deterministic consolidation for `first` item
        df_sorted = df.sort_values(by=['employee_name', 'start_datetime', 'item_number']).copy()

        # Iterate through unique groups based on common identifiers (excluding quantity and item_number)
        for _, group_df in df_sorted.groupby(group_cols):
            if len(group_df) == 1:
                # Single record - keep as is, fill quantity if missing
                record = group_df.iloc[0].copy()
                
                if pd.isna(record.get('reported_hours')) or record.get('reported_hours') == '':
                    duration = self.calculate_duration(
                        record['start_datetime'],
                        record['end_datetime'],
                        record['lunch_deduction']
                    )
                    record['reported_hours'] = duration
                    record['item_number'] = '1-MAN' # Default item type
                
                record['is_pre_split'] = False
                consolidated_records.append(record)
            else:
                # Multiple records for same time slot - consolidate them
                # This handles the scenario where one time block is split into regular/OT in source
                self._consolidate_group(group_df, consolidated_records)
        
        result_df = pd.DataFrame(consolidated_records)
        
        print(f"Consolidated {len(df)} raw records into {len(result_df)} consolidated records")
        return result_df
    

    def _consolidate_group(self, group_df: pd.DataFrame, consolidated_records: list):
        """Consolidate a group of records with the same time slot"""
        # Sort by item number to process in order (e.g., '1-MAN' before '1-MAN OT')
        group_df = group_df.sort_values(['item_number'], na_position='last')
        
        regular_parts = []
        ot_parts = []
        empty_parts = [] # For records where reported_hours is blank/NaN

        for _, record in group_df.iterrows():
            item_num = str(record.get('item_number', '')).strip()
            reported_hours = record.get('reported_hours') # Keep original as is
            
            if pd.isna(reported_hours) or str(reported_hours).strip() == '':
                empty_parts.append(record)
            elif 'OT' in item_num.upper():
                ot_parts.append(record)
            else:
                regular_parts.append(record)
        
        # Take the first record as the base, ensuring it includes validation_issues
        base_record = group_df.iloc[0].copy()
        
        # Sum up quantities based on discovered parts
        # FIXED: Use np.nan_to_num to handle potential NaN values from pd.to_numeric
        total_regular = sum(np.nan_to_num(pd.to_numeric(r.get('reported_hours', 0), errors='coerce')) for r in regular_parts)
        total_ot = sum(np.nan_to_num(pd.to_numeric(r.get('reported_hours', 0), errors='coerce')) for r in ot_parts)
        
        if (regular_parts or ot_parts) and not empty_parts: # It was explicitly split
            base_record['reported_hours'] = total_regular + total_ot
            base_record['pre_split_regular'] = total_regular
            base_record['pre_split_overtime'] = total_ot
            base_record['is_pre_split'] = True
            base_record['item_number'] = '1-MAN (PRE-SPLIT)' # Update item number for clarity
            
            self.calculation_log.append(
                f"Consolidated {base_record['employee_name']} {base_record['job_id']}: "
                f"{len(group_df)} records -> Pre-split Regular: {total_regular:.2f}h, Pre-split OT: {total_ot:.2f}h"
            )
        elif empty_parts: # Original was blank, calculate duration
            record_to_use = empty_parts[0].copy() # Use the first empty record as base
            duration = self.calculate_duration(
                record_to_use['start_datetime'],
                record_to_use['end_datetime'],
                record_to_use['lunch_deduction']
            )
            # Update base record with calculated duration
            base_record['reported_hours'] = duration
            base_record['is_pre_split'] = False
            base_record['item_number'] = record_to_use.get('item_number', '1-MAN') # Keep original or default
            self.calculation_log.append(
                f"Filled blank hours for {base_record['employee_name']} {base_record['job_id']}: Calculated {duration:.2f}h"
            )
        else: # Fallback, should ideally not happen if data is well-formed
            base_record['is_pre_split'] = False # Not explicitly pre-split
            self.calculation_log.append(
                f"Fallback consolidation for {base_record['employee_name']} {base_record['job_id']}: Used first record as is."
            )
        
        # Ensure validation_issues are carried over from the original group (they should be identical for group members)
        base_record['validation_issues'] = group_df['validation_issues'].iloc[0]

        consolidated_records.append(base_record)

    
    def calculate_duration(self, start_time: datetime, end_time: datetime, 
                          lunch_deduction: float = 0.0) -> float:
        """Calculate job duration with lunch deduction"""
        if pd.isna(start_time) or pd.isna(end_time):
            return 0.0
        
        duration = (end_time - start_time).total_seconds() / 3600.0
        
        # Ensure lunch_deduction is applied correctly (already cleaned in _clean_data)
        duration -= lunch_deduction
            
        # Ensure non-negative duration
        duration = max(0.0, duration)
        
        return duration
    
    def apply_rounding_rules(self, hours: float) -> float:
        """Apply AEP time rounding rules"""
        if pd.isna(hours) or hours <= 0:
            return 0.0
        
        whole_hours = int(hours)
        minutes = (hours - whole_hours) * 60
        
        # Apply rounding rules based on minutes
        if 0 <= minutes <= 6:
            rounded_minutes = 0
        elif 7 <= minutes <= 21:
            rounded_minutes = 15  # 0.25 hours
        elif 22 <= minutes <= 36:
            rounded_minutes = 30  # 0.50 hours
        elif 37 <= minutes <= 51:
            rounded_minutes = 45  # 0.75 hours
        else:  # 52-59 minutes - rounds up to next whole hour
            rounded_minutes = 0
            whole_hours += 1
        
        return whole_hours + (rounded_minutes / 60.0)
    
    def detect_overlaps(self, employee_jobs: pd.DataFrame) -> List[Dict]:
        """Detect overlapping time entries for an employee"""
        overlaps = []
        # Ensure jobs are sorted by start time for proper overlap detection
        jobs = employee_jobs.sort_values('start_datetime').copy()
        
        for i in range(len(jobs) - 1):
            current = jobs.iloc[i]
            next_job = jobs.iloc[i + 1]
            
            # An overlap occurs if the current job's end time is after the next job's start time
            # AND they are different jobs (to avoid flagging pre-splits as overlaps)
            if current['end_datetime'] > next_job['start_datetime'] and \
               current['job_id'] != next_job['job_id']: # Check for different job_ids
                
                # Calculate the actual overlap duration
                overlap_duration_td = current['end_datetime'] - next_job['start_datetime']
                overlap_duration_hours = overlap_duration_td.total_seconds() / 3600.0

                overlap_info = {
                    'job1_id': current['job_id'],
                    'job1_time': f"{current['start_datetime'].strftime('%Y-%m-%d %H:%M')} - {current['end_datetime'].strftime('%H:%M')}",
                    'job2_id': next_job['job_id'],
                    'job2_time': f"{next_job['start_datetime'].strftime('%Y-%m-%d %H:%M')} - {next_job['end_datetime'].strftime('%H:%M')}",
                    'overlap_duration': overlap_duration_hours
                }
                overlaps.append(overlap_info)
        
        return overlaps
    
    def process_employee_week(self, employee_name: str, week_data: pd.DataFrame) -> List[Dict]:
        """Process one employee's week of data applying all overtime rules"""
        results = []
        
        # Sort jobs chronologically
        week_data = week_data.sort_values(['work_date', 'start_datetime']).copy()
        
        # Detect overlaps and store job_ids that are part of an overlap.
        # Overlap jobs will have their hours zeroed out.
        overlaps = self.detect_overlaps(week_data)
        overlapping_job_ids = set()
        if overlaps:
            for overlap_detail in overlaps:
                # Add both jobs involved in the overlap to the set
                overlapping_job_ids.add(overlap_detail['job1_id'])
                overlapping_job_ids.add(overlap_detail['job2_id'])
                # Log overlap details in validation_exceptions for reporting
                self.validation_exceptions.append({
                    'employee_name': employee_name,
                    'job_id': f"{overlap_detail['job1_id']} & {overlap_detail['job2_id']}",
                    'client_job_number': 'N/A', # Specific client job numbers are for individual job entries
                    'contact_name': 'N/A',
                    'job_address': 'N/A',
                    'issues': f"Overlapping Shifts: {overlap_detail['job1_time']} overlaps with {overlap_detail['job2_time']} by {overlap_detail['overlap_duration']:.2f}h"
                })
                self.calculation_log.append(f"OVERLAP DETECTED for {employee_name}: {overlap_detail}")
        
        # Process each job
        for idx, job in week_data.iterrows():
            is_pre_split = job.get('is_pre_split', False)
            
            # Base dictionary for job results, carrying over validation_issues
            job_result = {
                'employee_name': employee_name,
                'job_id': job['job_id'],
                'work_date': job['work_date'],
                'day_of_week': job['day_of_week'],
                'week_start': job['week_start'],
                'start_time': job['start_datetime'],
                'end_time': job['end_datetime'],
                'lunch_deduction': job['lunch_deduction'],
                'is_call_out': job['is_call_out'],
                'is_pre_split': is_pre_split,
                'ot_reasons': [], # To be populated
                'regular_hours': 0.0, # Initialized
                'overtime_hours': 0.0, # Initialized
                'calculation_notes': '',
                'validation_issues': job.get('validation_issues', []) # Preserve existing validation issues
            }

            # NEW RULE: If job is part of an overlap, set hours to zero and skip further calculation for this job
            if job['job_id'] in overlapping_job_ids:
                job_result['overlap_status'] = 'Overlap'
                job_result['regular_hours'] = 0.0
                job_result['overtime_hours'] = 0.0
                job_result['calculation_notes'] = "Hours set to 0.0 due to overlapping shifts."
                results.append(job_result)
                continue # Skip to the next job in the loop
            else:
                job_result['overlap_status'] = '' # No overlap, or not the job causing it.

            # Step 1: Determine initial hours based on pre-split or raw duration
            if is_pre_split:
                pre_regular = float(job.get('pre_split_regular', 0))
                pre_ot = float(job.get('pre_split_overtime', 0))
                total_duration = pre_regular + pre_ot
                
                rounded_regular = self.apply_rounding_rules(pre_regular)
                rounded_ot = self.apply_rounding_rules(pre_ot)
                rounded_total = rounded_regular + rounded_ot
                
                job_result.update({
                    'raw_duration': total_duration,
                    'rounded_duration': rounded_total,
                    'regular_hours': rounded_regular,
                    'overtime_hours': rounded_ot,
                    'ot_reasons': ['Pre-Split in Source Data'] if rounded_ot > 0 else [],
                    'calculation_notes': f"Pre-split: {pre_regular:.2f}h reg + {pre_ot:.2f}h OT -> {rounded_regular:.2f}h reg + {rounded_ot:.2f}h OT",
                })
                
            else:
                raw_duration = self.calculate_duration(
                    job['start_datetime'], 
                    job['end_datetime'], 
                    job['lunch_deduction']
                )
                rounded_duration = self.apply_rounding_rules(raw_duration)
                
                job_result.update({
                    'raw_duration': raw_duration,
                    'rounded_duration': rounded_duration,
                    'regular_hours': rounded_duration, # Assume all regular for now
                    'overtime_hours': 0.0,
                    'calculation_notes': f"Raw: {raw_duration:.2f}h, Rounded: {rounded_duration:.2f}h" + (f", Lunch: {job['lunch_deduction']:.2f}h" if job['lunch_deduction'] > 0 else ""),
                })
            
            # Step 2: Apply Sunday and Call Out rules as absolute overrides
            # These rules convert ALL hours for the job to overtime if met.
            # This logic block now applies *after* initial hour assignment, regardless of pre-split status.
            
            # Check for Sunday Work
            if job_result['day_of_week'] == 'Sunday':
                # Convert all regular hours for this job to overtime
                if job_result['regular_hours'] > 0:
                    job_result['overtime_hours'] += job_result['regular_hours']
                    job_result['regular_hours'] = 0.0
                # Add 'Sunday Work' to reasons, even if it already had 'Pre-Split' OT
                if 'Sunday Work' not in job_result['ot_reasons']:
                    job_result['ot_reasons'].append('Sunday Work')
                
            # Check for Call Out (if not already Sunday Work)
            elif job_result['is_call_out']:
                # Convert all regular hours for this job to overtime
                if job_result['regular_hours'] > 0:
                    job_result['overtime_hours'] += job_result['regular_hours']
                    job_result['regular_hours'] = 0.0
                # Add 'Call Out' to reasons, even if it already had 'Pre-Split' OT
                if 'Call Out' not in job_result['ot_reasons']:
                    job_result['ot_reasons'].append('Call Out')

            results.append(job_result)
        
        # Step 3: Apply daily >10 hour rule (only to jobs that are not Sunday or Call-Out)
        # _apply_daily_over_10_rule already correctly filters based on ot_reasons, so no change needed here.
        results = self._apply_daily_over_10_rule(results)
        
        # Step 4: Apply weekly >40 hour rule (only to jobs that are not Sunday or Call-Out)
        # _apply_weekly_over_40_rule already correctly filters based on ot_reasons, so no change needed here.
        results = self._apply_weekly_over_40_rule(results)
        
        # Add week summary to each job.
        # Sums should reflect hours after all rules, including zeroing for overlaps.
        final_regular_total = sum(j['regular_hours'] for j in results)
        final_ot_total = sum(j['overtime_hours'] for j in results)
        
        for job in results:
            job['week_regular_total'] = final_regular_total
            job['week_ot_total'] = final_ot_total
        
        # Validation check - ensure no employee ends up with >40 regular hours per week
        if final_regular_total > 40.01: # Small tolerance for floating point arithmetic
            self.calculation_log.append(
                f"ERROR: {employee_name} has {final_regular_total:.2f} regular hours (over 40) after all rules!"
            )
        
        return results
    
    def _apply_daily_over_10_rule(self, jobs: List[Dict]) -> List[Dict]:
        """Apply over 10 hours in a day rule (skip jobs already fully OT by Sunday/Call-Out rules)"""
        daily_groups = {}
        for job in jobs:
            date = job['work_date']
            if date not in daily_groups:
                daily_groups[date] = []
            daily_groups[date].append(job)
        
        for date, day_jobs in daily_groups.items():
            # Filter for jobs eligible for daily OT conversion (not Sunday, Call-Out, or already fully OT)
            eligible_jobs = [job for job in day_jobs 
                           if job['regular_hours'] > 0 # Must have remaining regular hours
                           and 'Sunday Work' not in job['ot_reasons']
                           and 'Call Out' not in job['ot_reasons']
                           and job.get('overlap_status') != 'Overlap' # Exclude overlapping jobs
                           ]
            
            total_regular_day = sum(job['regular_hours'] for job in eligible_jobs)
            
            if total_regular_day > 10:
                excess = total_regular_day - 10
                
                # Apply excess to last job(s) of the day, working backward by start time
                day_jobs_sorted_reverse = sorted(eligible_jobs, key=lambda x: x['start_time'], reverse=True)
                
                remaining_excess = excess
                for job in day_jobs_sorted_reverse:
                    if job['regular_hours'] > 0 and remaining_excess > 0:
                        convertible = min(job['regular_hours'], remaining_excess)
                        
                        job['regular_hours'] -= convertible
                        job['overtime_hours'] += convertible
                        if 'Over 10 Hours/Day' not in job['ot_reasons']:
                            job['ot_reasons'].append('Over 10 Hours/Day')
                        
                        remaining_excess -= convertible
                        
                        if remaining_excess <= 0:
                            break
        
        return jobs
    
    def _apply_weekly_over_40_rule(self, jobs: List[Dict]) -> List[Dict]:
        """
        CRITICAL RULE: Apply weekly over 40 hours rule
        Ensures NO employee has more than 40 regular hours per week by converting excess to OT.
        """
        # Calculate total regular hours from ALL jobs that are eligible for conversion
        # Exclude Sunday and Call-Out jobs as they are already considered OT.
        # Exclude overlapping jobs as their hours are already zeroed out.
        eligible_for_conversion_jobs = [job for job in jobs
                                        if job['regular_hours'] > 0
                                        and 'Sunday Work' not in job['ot_reasons']
                                        and 'Call Out' not in job['ot_reasons']
                                        and job.get('overlap_status') != 'Overlap'
                                       ]
        
        total_regular_hours = sum(job['regular_hours'] for job in eligible_for_conversion_jobs)
        
        if total_regular_hours <= 40:
            return jobs # No excess regular hours this week
        
        # We have excess regular hours that must become overtime
        excess = total_regular_hours - 40
        
        # Sort eligible jobs by date/time (latest first) to apply rule backward through the week.
        # This prioritizes converting the most recent regular hours to OT.
        jobs_sorted_reverse = sorted(eligible_for_conversion_jobs, 
                                     key=lambda x: (x['work_date'], x['start_time']), reverse=True)
        
        remaining_excess = excess
        converted_job_ids = set() # Track unique jobs converted for logging
        
        for job in jobs_sorted_reverse:
            if remaining_excess <= 0:
                break
                
            if job['regular_hours'] > 0:
                convertible = min(job['regular_hours'], remaining_excess)
                
                job['regular_hours'] -= convertible
                job['overtime_hours'] += convertible
                if 'Over 40 Hours/Week' not in job['ot_reasons']:
                    job['ot_reasons'].append('Over 40 Hours/Week')
                
                remaining_excess -= convertible
                converted_job_ids.add(job['job_id'])
                
                # Update calculation notes (append to existing notes)
                if job['calculation_notes']:
                    job['calculation_notes'] += f" | Converted {convertible:.2f}h reg->OT (weekly >40)"
                else:
                    job['calculation_notes'] = f"Converted {convertible:.2f}h reg->OT (weekly >40)"
        
        if converted_job_ids:
            self.calculation_log.append(
                f"Weekly >40 rule applied: Converted {excess:.2f}h regular->OT for {len(converted_job_ids)} jobs"
            )
        
        return jobs
    
    def process_all_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process all employee data"""
        all_results = []
        
        # Group by employee and week
        for employee in df['employee_name'].unique():
            employee_data = df[df['employee_name'] == employee]
            
            for week_start in employee_data['week_start'].unique():
                week_data = employee_data[employee_data['week_start'] == week_start]
                
                print(f"Processing {employee} - Week of {week_start}")
                week_results = self.process_employee_week(employee, week_data)
                all_results.extend(week_results)
        
        # Convert to DataFrame
        results_df = pd.DataFrame(all_results)
        
        # Create filtered data DataFrame
        filtered_df = pd.DataFrame(self.filtered_data) if self.filtered_data else pd.DataFrame()
        
        return results_df, filtered_df
    
    def _create_summary(self, results_df: pd.DataFrame) -> List[Dict]:
        """Create summary statistics per employee"""
        summary = []
        
        for employee in results_df['employee_name'].unique():
            emp_data = results_df[results_df['employee_name'] == employee]
            
            # Count OT reasons for this employee
            ot_reason_counts = {}
            for reasons_list in emp_data['ot_reasons']:
                for reason in reasons_list:
                    ot_reason_counts[reason] = ot_reason_counts.get(reason, 0) + 1
            
            # Calculate per-week totals
            week_groups = emp_data.groupby('week_start')
            weekly_regular_hours = []
            weekly_overtime_hours = []
            
            for week_start, week_data in week_groups:
                week_regular = week_data['regular_hours'].sum()
                week_ot = week_data['overtime_hours'].sum()
                weekly_regular_hours.append(week_regular)
                weekly_overtime_hours.append(week_ot)
            
            # Calculate overall totals and maximums for summary
            total_regular_hours_cumulative = sum(weekly_regular_hours) # True sum across all weeks
            total_overtime_hours_cumulative = sum(weekly_overtime_hours) # True sum across all weeks
            
            # Max weekly regular is the key compliance metric for AEP's 40h rule
            max_weekly_regular = max(weekly_regular_hours) if weekly_regular_hours else 0.0
            max_weekly_overtime = max(weekly_overtime_hours) if weekly_overtime_hours else 0.0
            
            # Check compliance: Max weekly regular should never exceed 40
            is_compliant = max_weekly_regular <= 40.01 # Small tolerance for floating point
            
            summary.append({
                'employee_name': employee,
                'total_regular_hours': total_regular_hours_cumulative,  # Cumulative for all weeks processed
                'total_overtime_hours': total_overtime_hours_cumulative,  # Cumulative for all weeks processed
                'total_hours': total_regular_hours_cumulative + total_overtime_hours_cumulative,
                'max_weekly_regular': max_weekly_regular, # Key compliance metric
                'max_weekly_overtime': max_weekly_overtime,
                'weeks_worked': len(weekly_regular_hours),
                'jobs_processed': len(emp_data),
                'call_out_jobs': emp_data['is_call_out'].sum(), # Count jobs flagged as call out
                'sunday_hours_ot': emp_data[emp_data['day_of_week'] == 'Sunday']['overtime_hours'].sum(),
                'over_10_day_jobs_flagged': ot_reason_counts.get('Over 10 Hours/Day', 0),
                'over_40_week_jobs_flagged': ot_reason_counts.get('Over 40 Hours/Week', 0),
                'pre_split_jobs_flagged': ot_reason_counts.get('Pre-Split in Source Data', 0),
                'compliance_status': 'COMPLIANT' if is_compliant else 'NON-COMPLIANT',
                'weekly_regular_breakdown': f"{len(weekly_regular_hours)} weeks: " + ", ".join([f"{h:.1f}h" for h in weekly_regular_hours]) if len(weekly_regular_hours) > 1 else (f"{max_weekly_regular:.1f}h" if weekly_regular_hours else "0.0h")
            })
        
        return summary
    
    def export_results(self, results_df: pd.DataFrame, filtered_df: pd.DataFrame, 
                      output_file: str = 'aep_overtime_results.xlsx'):
        """Export results to Excel file with multiple sheets"""
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main results sheet - format for easy reading
            export_df = results_df.copy()
            export_df['ot_reasons_text'] = export_df['ot_reasons'].apply(lambda x: ', '.join(x) if x else '')
            # Convert validation_issues list to string for display
            export_df['validation_issues_text'] = export_df['validation_issues'].apply(lambda x: ', '.join(x) if x else '')
            
            # Reorder columns for better presentation
            column_order = [
                'employee_name', 'work_date', 'day_of_week', 'week_start', 'job_id',
                'start_time', 'end_time', 'raw_duration', 'rounded_duration',
                'regular_hours', 'overtime_hours', 'ot_reasons_text',
                'is_call_out', 'lunch_deduction', 'is_pre_split', 'overlap_status',
                'validation_issues_text', # Added new column for job-level validation issues
                'calculation_notes',
                'week_regular_total', 'week_ot_total',
                'client_job_number', 'bill_to_account', 'contact_name', 'job_address' # Include original columns for context
            ]
            
            # Only include columns that exist in the DataFrame after processing
            available_columns = [col for col in column_order if col in export_df.columns]
            export_df = export_df[available_columns]
            
            export_df.to_excel(writer, sheet_name='final_data', index=False)
            
            # Filtered records sheet (records removed due to missing core data)
            if not filtered_df.empty:
                filtered_df.to_excel(writer, sheet_name='filtered_records', index=False)
            
            # Summary sheet
            summary_data = self._create_summary(results_df)
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='employee_summary', index=False)
            
            # NEW: Detailed validation exceptions sheet
            if self.validation_exceptions:
                # Ensure each exception entry has all relevant fields for display
                # Note: 'issues' itself is a string of comma-separated issues
                validation_exceptions_df = pd.DataFrame(self.validation_exceptions)
                # Sort by employee and then job_id for readability
                validation_exceptions_df = validation_exceptions_df.sort_values(
                    by=['employee_name', 'job_id']
                )
                validation_exceptions_df.to_excel(writer, sheet_name='validation_exceptions', index=False)
            
            # Detailed report as text
            report_text = self.generate_detailed_report(results_df, filtered_df)
            report_df = pd.DataFrame({'Report': [report_text]})
            report_df.to_excel(writer, sheet_name='detailed_report', index=False)
        
        print(f"Results exported to {output_file}")
        return output_file
    
    def generate_detailed_report(self, results_df: pd.DataFrame, filtered_df: pd.DataFrame) -> str:
        """Generate a detailed text report"""
        report = []
        report.append("AEP OVERTIME CALCULATION REPORT")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Overall summary
        total_employees = results_df['employee_name'].nunique()
        total_jobs = len(results_df)
        total_regular = results_df['regular_hours'].sum()
        total_ot = results_df['overtime_hours'].sum()
        
        report.append("OVERALL SUMMARY")
        report.append("-" * 20)
        report.append(f"Total Employees: {total_employees}")
        report.append(f"Total Jobs Processed (after filtering/deduplication): {total_jobs}")
        report.append(f"Total Regular Hours: {total_regular:.2f}")
        report.append(f"Total Overtime Hours: {total_ot:.2f}")
        report.append(f"Total Hours: {(total_regular + total_ot):.2f}")
        report.append("")
        
        # Compliance Check: No weeks should exceed 40 regular hours
        emp_summary_for_compliance = results_df.groupby(['employee_name', 'week_start'])['regular_hours'].sum().reset_index()
        over_40_weeks = emp_summary_for_compliance[emp_summary_for_compliance['regular_hours'] > 40.01] # Use small tolerance
        
        if len(over_40_weeks) > 0:
            report.append("⚠️  WEEKS WITH >40 REGULAR HOURS (ERROR - SHOULD BE CONVERTED TO OT)")
            report.append("-" * 60)
            for _, week in over_40_weeks.iterrows():
                report.append(f"{week['employee_name']} - Week {week['week_start']}: {week['regular_hours']:.2f}h")
            report.append("")
        else:
            report.append("✅ VALIDATION: No individual weeks exceed 40 regular hours for any employee.")
            report.append("")
        
        # Overtime Breakdown
        ot_reasons = {}
        for reasons_list in results_df['ot_reasons']:
            for reason in reasons_list:
                ot_reasons[reason] = ot_reasons.get(reason, 0) + 1
        
        if ot_reasons:
            report.append("OVERTIME BREAKDOWN (by reason)")
            report.append("-" * 30)
            for reason, count in sorted(ot_reasons.items()):
                ot_hours_for_reason = results_df[results_df['ot_reasons'].apply(lambda x: reason in x)]['overtime_hours'].sum()
                report.append(f"{reason}: {count} jobs, {ot_hours_for_reason:.2f} hours")
            report.append("")
        
        # NEW: Job Validation Exceptions Summary
        if self.validation_exceptions:
            report.append("🔴 JOB VALIDATION EXCEPTIONS FOUND")
            report.append("-" * 30)
            exception_counts = {}
            for exc in self.validation_exceptions:
                # 'issues' could be a single string or a list, ensure it's iterable
                issues_content = exc['issues']
                if isinstance(issues_content, str):
                    issues_list = issues_content.split(', ') # Assuming comma-separated for simple cases
                else: # Assume it's already a list from _validate_job_record
                    issues_list = issues_content
                    
                for issue in issues_list:
                    exception_counts[issue] = exception_counts.get(issue, 0) + 1
            
            for reason, count in sorted(exception_counts.items()):
                report.append(f"- {reason}: {count} jobs")
            report.append(f"\nSee 'validation_exceptions' sheet for detailed list of {len(self.validation_exceptions)} problematic jobs.")
            report.append("")
        else:
            report.append("✅ VALIDATION: No specific job validation exceptions found.")
            report.append("")
            
        # Filtered Records Summary (records removed due to missing core data during parsing)
        if not filtered_df.empty:
            report.append("FILTERED RECORDS (DUE TO MISSING/INVALID CORE DATA)")
            report.append("-" * 50)
            filter_reasons = filtered_df['filter_reason'].value_counts()
            for reason, count in filter_reasons.items():
                report.append(f"{reason}: {count} records")
            report.append("")
        
        return "\n".join(report)

# Flask Routes (No changes needed to core routes as they rely on the class logic)
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            # Save uploaded file temporarily
            filename = secure_filename(file.filename)
            temp_dir = tempfile.mkdtemp()
            input_file_path = os.path.join(temp_dir, filename)
            file.save(input_file_path)
            
            # Process the file
            calculator = AEPOvertimeCalculator()
            
            # Parse input file
            df = calculator.parse_input_file(input_file_path)
            
            # Process all data
            results_df, filtered_df = calculator.process_all_data(df)
            
            # Create output file
            output_filename = f"aep_overtime_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            output_file_path = os.path.join(temp_dir, output_filename)
            calculator.export_results(results_df, filtered_df, output_file_path)
            
            # Generate summary for display on the webpage
            summary_data_for_display = calculator._create_summary(results_df)
            detailed_report_for_display = calculator.generate_detailed_report(results_df, filtered_df)
            
            # Calculate key metrics for main summary display
            total_employees = results_df['employee_name'].nunique()
            total_jobs = len(results_df)
            total_regular = results_df['regular_hours'].sum()
            total_ot = results_df['overtime_hours'].sum()
            
            # Check compliance based on the summary metric
            emp_summary_check = results_df.groupby(['employee_name', 'week_start'])['regular_hours'].sum().reset_index()
            over_40_weeks_count = (emp_summary_check['regular_hours'] > 40.01).sum()
            compliance_status = "COMPLIANT" if over_40_weeks_count == 0 else "NON-COMPLIANT"
            
            # Store file path in session for download
            from flask import session
            session['output_file'] = output_file_path
            session['output_filename'] = output_filename
            
            return render_template('results.html', 
                                 summary={
                                     'total_employees': total_employees,
                                     'total_jobs': total_jobs,
                                     'total_regular': total_regular,
                                     'total_ot': total_ot,
                                     'total_hours': total_regular + total_ot,
                                     'compliance_status': compliance_status,
                                     'over_40_weeks': over_40_weeks_count,
                                     'validation_issues_found': len(calculator.validation_exceptions) > 0 # Indicate if any issues found
                                 },
                                 employee_summary=summary_data_for_display[:10],  # Show top 10 employees
                                 detailed_report=detailed_report_for_display,
                                 download_filename=output_filename)
            
        else:
            flash('Invalid file type. Please upload CSV, XLS, or XLSX files only.')
            return redirect(request.url)
            
    except Exception as e:
        flash(f'Error processing file: {str(e)}')
        print(f"Error: {e}")
        traceback.print_exc()
        return redirect(url_for('index'))

@app.route('/download')
def download_file():
    try:
        from flask import session
        if 'output_file' not in session:
            flash('No file available for download')
            return redirect(url_for('index'))
        
        output_file_path = session['output_file']
        output_filename = session['output_filename']
        
        if not os.path.exists(output_file_path):
            flash('File not found')
            return redirect(url_for('index'))
        
        return send_file(output_file_path, 
                        as_attachment=True, 
                        download_name=output_filename,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    except Exception as e:
        flash(f'Error downloading file: {str(e)}')
        return redirect(url_for('index'))

# HTML Templates (create templates folder and save these as separate files)
# These templates are embedded for a single file setup, but should typically be in a 'templates' directory.
@app.route('/get_template/<template_name>')
def get_template(template_name):
    """Endpoint to serve template content for development"""
    templates = {
        'base.html': '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}AEP Overtime Calculator{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
    <style>
        .upload-area {
            border: 2px dashed #007bff;
            border-radius: 10px;
            padding: 40px;
            text-align: center;
            background-color: #f8f9fa;
            transition: all 0.3s ease;
        }
        .upload-area:hover {
            border-color: #0056b3;
            background-color: #e3f2fd;
        }
        .summary-card {
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-radius: 8px;
        }
        .compliance-badge {
            font-size: 0.9em;
            padding: 0.5em 1em;
        }
        .report-section {
            background-color: #f8f9fa;
            border-left: 4px solid #007bff;
            padding: 15px;
            margin: 15px 0;
            overflow-x: auto; /* Allow horizontal scrolling for wide reports */
            white-space: pre-wrap; /* Preserve whitespace and wrap long lines */
        }
    </style>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="{{ url_for('index') }}">
                <strong>AEP Overtime Calculator</strong>
            </a>
        </div>
    </nav>

    <div class="container mt-4">
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div class="alert alert-warning alert-dismissible fade show" role="alert">
                        {{ message }}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    </div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        {% block content %}{% endblock %}
    </div>

    <footer class="bg-light mt-5 py-4">
        <div class="container">
            <div class="text-center text-muted">
                <small>AEP Overtime Calculator - Processes employee timesheet data and applies AEP overtime rules</small>
            </div>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>''',
        
        'index.html': '''{% extends "base.html" %}

{% block content %}
<div class="row justify-content-center">
    <div class="col-lg-8">
        <div class="card shadow">
            <div class="card-header bg-primary text-white">
                <h2 class="card-title mb-0">
                    <i class="fas fa-calculator"></i> Upload Timesheet Data
                </h2>
            </div>
            <div class="card-body">
                <div class="mb-4">
                    <h5>Instructions:</h5>
                    <ul class="list-unstyled">
                        <li>✓ Upload your timesheet file (CSV, XLS, or XLSX format)</li>
                        <li>✓ The system will apply AEP overtime rules automatically</li>
                        <li>✓ Download the processed results with detailed calculations</li>
                    </ul>
                </div>

                <form action="{{ url_for('upload_file') }}" method="post" enctype="multipart/form-data">
                    <div class="upload-area mb-4">
                        <div class="mb-3">
                            <i class="fas fa-cloud-upload-alt fa-3x text-primary mb-3"></i>
                            <h4>Choose File to Upload</h4>
                            <p class="text-muted">Select your timesheet data file (CSV, XLS, XLSX)</p>
                        </div>
                        <input type="file" 
                               class="form-control form-control-lg" 
                               name="file" 
                               accept=".csv,.xls,.xlsx"
                               required>
                    </div>
                    
                    <div class="text-center">
                        <button type="submit" class="btn btn-primary btn-lg px-5">
                            <i class="fas fa-cogs"></i> Process Overtime Data
                        </button>
                    </div>
                </form>

                <div class="mt-4 p-3 bg-light rounded">
                    <h6 class="text-primary">AEP Overtime Rules Applied:</h6>
                    <ul class="small mb-0">
                        <li>Sunday work = All overtime (highest priority)</li>
                        <li>Call-out work = All overtime (highest priority)</li>
                        <li>Over 10 hours/day = Overtime for excess</li>
                        <li>Over 40 hours/week = Overtime for excess</li>
                        <li>Time rounding per AEP standards</li>
                    </ul>
                    <h6 class="text-primary mt-2">Data Validation Checks:</h6>
                    <ul class="small mb-0">
                        <li>Duplicate job entries</li>
                        <li>Overlapping shifts (hours set to zero)</li>
                        <li>Invalid work order formats/numbers</li>
                        <li>Line shop addresses used as work locations</li>
                        <li>Invalid contact names for sign-off</li>
                    </ul>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}''',
        
        'results.html': '''{% extends "base.html" %}

{% block title %}Processing Results - AEP Overtime Calculator{% endblock %}

{% block content %}
<div class="row">
    <div class="col-12">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <h2 class="text-success">
                <i class="fas fa-check-circle"></i> Processing Complete!
            </h2>
            <a href="{{ url_for('download_file') }}" class="btn btn-success btn-lg">
                <i class="fas fa-download"></i> Download Results ({{ download_filename }})
            </a>
        </div>
    </div>
</div>

<!-- Summary Cards -->
<div class="row mb-4">
    <div class="col-md-2">
        <div class="card summary-card text-center">
            <div class="card-body">
                <h3 class="text-primary">{{ summary.total_employees }}</h3>
                <p class="card-text">Employees</p>
            </div>
        </div>
    </div>
    <div class="col-md-2">
        <div class="card summary-card text-center">
            <div class="card-body">
                <h3 class="text-info">{{ summary.total_jobs }}</h3>
                <p class="card-text">Jobs</p>
            </div>
        </div>
    </div>
    <div class="col-md-2">
        <div class="card summary-card text-center">
            <div class="card-body">
                <h3 class="text-success">{{ "%.1f"|format(summary.total_regular) }}</h3>
                <p class="card-text">Regular Hours</p>
            </div>
        </div>
    </div>
    <div class="col-md-2">
        <div class="card summary-card text-center">
            <div class="card-body">
                <h3 class="text-warning">{{ "%.1f"|format(summary.total_ot) }}</h3>
                <p class="card-text">Overtime Hours</p>
            </div>
        </div>
    </div>
    <div class="col-md-2">
        <div class="card summary-card text-center">
            <div class="card-body">
                <h3 class="text-secondary">{{ "%.1f"|format(summary.total_hours) }}</h3>
                <p class="card-text">Total Hours</p>
            </div>
        </div>
    </div>
    <div class="col-md-2">
        <div class="card summary-card text-center">
            <div class="card-body">
                {% if summary.compliance_status == "COMPLIANT" %}
                    <h3 class="text-success">✓</h3>
                    <p class="card-text">Compliant</p>
                {% else %}
                    <h3 class="text-danger">⚠</h3>
                    <p class="card-text">Issues Found (Review Excel)</p>
                {% endif %}
                {% if summary.validation_issues_found %}
                    <h3 class="text-warning">!</h3>
                    <p class="card-text">Data Exceptions</p>
                {% endif %}
            </div>
        </div>
    </div>
</div>

<!-- Employee Summary Table -->
<div class="row mb-4">
    <div class="col-12">
        <div class="card">
            <div class="card-header">
                <h4>Employee Summary (Top 10)</h4>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>Employee</th>
                                <th>Max Weekly Regular</th>
                                <th>Total OT Hours</th>
                                <th>Total Jobs</th>
                                <th>Weeks Worked</th>
                                <th>Compliance</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for emp in employee_summary %}
                            <tr>
                                <td><strong>{{ emp.employee_name }}</strong></td>
                                <td>{{ "%.1f"|format(emp.max_weekly_regular) }}h</td>
                                <td>{{ "%.1f"|format(emp.total_overtime_hours) }}h</td>
                                <td>{{ emp.jobs_processed }}</td>
                                <td>{{ emp.weeks_worked }}</td>
                                <td>
                                    {% if emp.compliance_status == "COMPLIANT" %}
                                        <span class="badge bg-success compliance-badge">✓ Compliant</span>
                                    {% else %}
                                        <span class="badge bg-warning compliance-badge">⚠ Check</span>
                                    {% endif %}
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>

<!-- Detailed Report -->
<div class="row">
    <div class="col-12">
        <div class="card">
            <div class="card-header">
                <h4>Detailed Report</h4>
            </div>
            <div class="card-body">
                <pre class="report-section">{{ detailed_report }}</pre>
            </div>
        </div>
    </div>
</div>

<!-- Action Buttons -->
<div class="row mt-4 mb-5">
    <div class="col-12 text-center">
        <a href="{{ url_for('download_file') }}" class="btn btn-success btn-lg me-3">
            <i class="fas fa-download"></i> Download Excel Results
        </a>
        <a href="{{ url_for('index') }}" class="btn btn-outline-primary btn-lg">
            <i class="fas fa-upload"></i> Process Another File
        </a>
    </div>
</div>
{% endblock %}'''
    }
    
    return templates.get(template_name, "Template not found")

if __name__ == '__main__':
    # Create templates directory and files if they don't exist
    templates_dir = 'templates'
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
    
    # Create template files
    templates = {
        'base.html': get_template('base.html'),
        'index.html': get_template('index.html'), 
        'results.html': get_template('results.html')
    }
    
    for filename, content in templates.items():
        filepath = os.path.join(templates_dir, filename)
        if not os.path.exists(filepath):
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
    
    # Configure Flask app for development
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.config['SESSION_TYPE'] = 'filesystem'
    
    print("Starting AEP Overtime Calculator Flask App...")
    print("Open your web browser and go to: http://localhost:4444") # Changed port to 4444 to avoid common conflicts
    print("Upload your timesheet file to process overtime calculations")
    
    app.run(debug=True, host='0.0.0.0', port=4444)