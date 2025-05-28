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
from io import BytesIO
import traceback # Import traceback for detailed error logging

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
        self.filtered_data = [] # For records dropped due to missing core data
        self.calculation_log = [] # For internal process messages
        self.validation_exceptions = [] # For records flagged with specific validation issues

        # Validation rules (hardcoded as per requirements)
        # Line shop addresses: If a job address or name contains any of these, it's flagged.
        self.LINE_SHOP_ADDRESSES = [
            "4636 Ardmore Ave", "6900 N 600 W", "15650 Edgerton Rd",
            "2101 IN-28", "3802 South Meeker Ave", "4421 Ardmore Ave",
            "404 S Frances St", "2825 Prairie Ave"
        ]
        
        # Work Order Validation:
        # Regex for acceptable core work order numbers (e.g., DAP0413021)
        # It matches 3 letters (case-insensitive, e.g., DAP, DKP, DIM, DKY, BKP, BAP)
        # followed by exactly 7 digits.
        self.CORE_WORK_ORDER_REGEX = re.compile(r'([A-Z]{3}\d{7})', re.IGNORECASE)

        # Invalid contact names for sign-off as specified in transcripts.
        self.INVALID_CONTACT_NAMES = {'PAT DENNEY', 'PAT DENNY'}

        # NOTE: self.VALID_WORK_ORDER_NUMBERS, self.BILL_TO_ACCOUNT_REGION_MAP,
        # and self.SPECIFIC_WORK_ORDER_LENGTH are no longer used for Client Job # validation
        # as per latest requirements. The validation for Client Job # is now based on:
        # 1. Whether a core '3 letters + 7 digits' pattern can be extracted.
        # 2. Whether the total length of the 'Client Job #' string is more than 10 characters.


    def parse_input_file(self, file_path: str) -> pd.DataFrame:
        """Parse input spreadsheet file and return standardized DataFrame"""
        try:
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.lower().endswith(('.xls', '.xlsx')):
                # Try reading as Excel first. If it's an older .xls that's actually HTML, try HTML parsing.
                try:
                    df = pd.read_excel(file_path)
                except Exception as e:
                    print(f"Failed to read as Excel ({e}), attempting to parse as HTML table.")
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    df = self._parse_html_table(content)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
                
            df = self._standardize_columns(df)
            print(f"Successfully loaded {len(df)} records from {file_path}")
            return df
            
        except Exception as e:
            print(f"Error parsing file {file_path}: {e}")
            raise
    
    def _parse_html_table(self, html_content: str) -> pd.DataFrame:
        """Parse HTML table format (for files like the sample .xls that might be HTML)"""
        import re
        from html import unescape
        
        rows = []
        row_pattern = r'<tr>(.*?)</tr>'
        cell_pattern = r'<t[hd][^>]*>(.*?)</t[hd]>'
        
        for row_match in re.finditer(row_pattern, html_content, re.DOTALL | re.IGNORECASE):
            row_html = row_match.group(1)
            cells = []
            
            for cell_match in re.finditer(cell_pattern, row_html, re.DOTALL | re.IGNORECASE):
                cell_content = cell_match.group(1)
                cell_content = re.sub(r'<[^>]+>', '', cell_content)
                cell_content = unescape(cell_content).strip()
                cells.append(cell_content)
            
            if cells:
                rows.append(cells)
        
        if not rows:
            raise ValueError("No table data found in HTML file")
        
        headers = rows[0]
        data_rows = rows[1:]
        
        df = pd.DataFrame(data_rows, columns=headers)
        return df
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and data types"""
        column_mapping = {
            'Resource Name': 'employee_name',
            'Validated Start Time': 'start_time',
            'Validated End Time': 'end_time',
            'Call Out': 'is_call_out',
            'Lunch Deduction': 'lunch_deduction',
            'Job Name': 'job_id', # This is usually the internal job number
            'Quantity': 'reported_hours',
            'Item Number': 'item_number',
            'Client Job #': 'client_job_number', # This is the AEP work order number
            'Contact: Full Name': 'contact_name', # For Pat Denny validation
            'Bill To Account: Account Name': 'bill_to_account', # For region context (kept for potential future rules)
            # 'Job Address': 'job_address' # Uncomment if a dedicated 'Job Address' column exists in source data
        }
        
        df_renamed = df.rename(columns=column_mapping)
        
        # Define all columns expected or required by the processing logic
        # This list includes original columns that are needed for data context or validation.
        required_columns = [
            'employee_name', 'start_time', 'end_time', 'is_call_out', 
            'lunch_deduction', 'job_id', 'reported_hours', 'item_number',
            'client_job_number', 'contact_name', 'bill_to_account', 'job_address', # 'job_address' is a placeholder
            'Line_Note' # Included for Line Shop Address validation
        ]
        
        # Add missing columns with default values to prevent KeyError later
        for col in required_columns:
            if col not in df_renamed.columns:
                if col == 'employee_name':
                    df_renamed[col] = ''
                elif col in ['reported_hours', 'lunch_deduction']:
                    df_renamed[col] = 0.0
                elif col == 'is_call_out':
                    df_renamed[col] = '0' # Default to not a call out boolean initially
                else: # For other string columns
                    df_renamed[col] = ''
        
        # Perform cleaning, data type conversions, and initial validations
        df_renamed = self._clean_data(df_renamed)
        
        return df_renamed
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and validates data:
        - Filters out records with missing essential info.
        - Detects and logs exact duplicate records, removing them from main processing.
        - Refines 'is_call_out' flag based on 'Item Number'.
        - Filters out records with invalid date/time formats.
        - Performs job-level validations (line shop, work order, invalid contact).
        - Consolidates pre-split records.
        """
        initial_record_count = len(df)
        
        # Step 1: Filter out rows with missing essential data (employee name, start/end times)
        df_cleaned = df.dropna(subset=['employee_name', 'start_time', 'end_time']).copy()
        df_cleaned = df_cleaned[df_cleaned['employee_name'].str.strip() != ''].copy()
        
        if len(df_cleaned) < initial_record_count:
            filtered_essential_count = initial_record_count - len(df_cleaned)
            print(f"Filtered out {filtered_essential_count} records due to missing essential data (employee name or times).")
            # For comprehensive logging, these filtered rows could also be added to self.filtered_data
            # if df_cleaned.empty: return pd.DataFrame() # Handle case where all rows are filtered
        
        # Step 2: Detect and log exact duplicate records, then remove duplicates for processing accuracy.
        # This prevents double-counting hours for identical entries.
        # Columns that define a unique "record" to detect true duplicates
        duplicate_check_cols = [
            'employee_name', 'job_id', 'start_time', 'end_time', 
            'reported_hours', 'item_number', 'is_call_out', 'lunch_deduction',
            'client_job_number', 'contact_name', 'bill_to_account', 'job_address', 'Line_Note'
        ]
        # Ensure all columns exist before using them in duplicated()
        for col in duplicate_check_cols:
            if col not in df_cleaned.columns:
                df_cleaned[col] = '' # Add empty column if not present
        
        # Find all occurrences of duplicated rows (keep=False marks all as True)
        duplicated_rows_identified = df_cleaned[df_cleaned.duplicated(subset=duplicate_check_cols, keep=False)].copy()
        
        if not duplicated_rows_identified.empty:
            print(f"Found {len(duplicated_rows_identified)} instances of exact duplicate records. Logging as validation exceptions.")
            # Log each unique duplicate set only once in validation_exceptions for clarity
            for _, row in duplicated_rows_identified.drop_duplicates(subset=duplicate_check_cols, keep='first').iterrows():
                self.validation_exceptions.append({
                    'employee_name': row['employee_name'],
                    'job_id': row['job_id'],
                    'client_job_number': row.get('client_job_number', ''),
                    'contact_name': row.get('contact_name', ''),
                    'job_address': row.get('job_address', ''), # Use the actual job_address if present
                    'issues': 'Exact Duplicate Record (only first instance processed)'
                })
            # Remove duplicates, keeping only the first occurrence for accurate calculations
            df_cleaned = df_cleaned.drop_duplicates(subset=duplicate_check_cols, keep='first').copy()
            print(f"Removed exact duplicate records for processing. Remaining unique records: {len(df_cleaned)}.")

        # Step 3: Refine 'is_call_out' flag.
        # Convert to string and strip whitespace for robust comparison.
        df_cleaned['is_call_out'] = df_cleaned['is_call_out'].astype(str).str.strip()
        df_cleaned['item_number'] = df_cleaned['item_number'].astype(str).str.strip() 
        
        # Set 'is_call_out' to True if the 'Call Out' column explicitly indicates it, OR
        # if 'EMEG' (Emergency) is found in the 'Item Number' field.
        df_cleaned['is_call_out'] = df_cleaned.apply(lambda row: \
            row['is_call_out'].lower() == 'true' or \
            'yes' in row['is_call_out'].lower() or \
            'emeg' in row['item_number'].lower(),
            axis=1
        )
        
        # Step 4: Process and clean 'lunch_deduction'.
        df_cleaned['lunch_deduction'] = pd.to_numeric(df_cleaned['lunch_deduction'], errors='coerce').fillna(0.0)
        df_cleaned['lunch_deduction'] = df_cleaned['lunch_deduction'].abs() # Convert negative deductions to positive
        df_cleaned['lunch_deduction'] = df_cleaned['lunch_deduction'].clip(upper=1.0) # Cap at 1 hour (as 0.5 is typical)

        # Step 5: Parse datetime columns and filter out invalid date/time entries.
        df_cleaned['start_datetime'] = pd.to_datetime(df_cleaned['start_time'], errors='coerce')
        df_cleaned['end_datetime'] = pd.to_datetime(df_cleaned['end_time'], errors='coerce')
        
        valid_date_time_entries = df_cleaned['start_datetime'].notna() & df_cleaned['end_datetime'].notna()
        invalid_datetime_count = (~valid_date_time_entries).sum()
        if invalid_datetime_count > 0:
            print(f"Filtered out {invalid_datetime_count} records due to invalid date/time data.")
            for idx, row in df_cleaned[~valid_date_time_entries].iterrows():
                self.filtered_data.append({
                    'employee_name': row['employee_name'],
                    'job_id': row['job_id'],
                    'start_time': row['start_time'],
                    'end_time': row['end_time'],
                    'filter_reason': 'Invalid date/time format'
                })
        df_cleaned = df_cleaned[valid_date_time_entries].copy()
        
        # Step 6: Perform job-level specific validations (Line Shop Address, Work Order Format, Invalid Contact).
        # 'validation_issues' column will store a list of strings if issues are found for that row.
        df_cleaned['validation_issues'] = df_cleaned.apply(self._validate_job_record, axis=1)
        
        # Collect all jobs that have validation issues into the dedicated exceptions list.
        # NOTE: Duplicates identified in Step 2 are already logged. This collects issues from Step 6.
        for idx, row in df_cleaned[df_cleaned['validation_issues'].apply(bool)].iterrows():
            self.validation_exceptions.append({
                'employee_name': row['employee_name'],
                'job_id': row['job_id'],
                'client_job_number': row.get('client_job_number', ''),
                'contact_name': row.get('contact_name', ''),
                'job_address': row.get('job_address', ''),
                'issues': ', '.join(row['validation_issues']) # Convert list of issues to a single string
            })
            
        # Step 7: Consolidate pre-split records (e.g., 1-MAN and 1-MAN OT split in source data).
        # This should happen after initial cleaning and validation flagging.
        df_final = self._consolidate_split_records(df_cleaned)
        
        # Step 8: Add derived columns for time calculations (date, day of week, week start).
        df_final['work_date'] = df_final['start_datetime'].dt.date
        df_final['day_of_week'] = df_final['start_datetime'].dt.day_name()
        
        # Calculate Sunday-to-Saturday weeks (AEP standard week definition)
        df_final['week_start'] = df_final['start_datetime'].dt.date - pd.to_timedelta((df_final['start_datetime'].dt.dayofweek + 1) % 7, unit='D')

        return df_final
    
    def _validate_job_record(self, row: pd.Series) -> List[str]:
        """
        Validates individual job records against specific business rules:
        - Line Shop Addresses for work locations.
        - Work Order (Client Job #) format and length.
        - Invalid Contact Names for sign-off.
        Returns a list of issue descriptions if problems are found.
        """
        issues = []
        
        # 1. Validate against line shop addresses
        # This check looks for presence of known line shop addresses in job_address, job_id, or Line_Note.
        job_address = str(row.get('job_address', '')).strip() # Assumes a job_address column might exist
        job_id = str(row.get('job_id', '')).strip() # Sometimes addresses might be in Job Name
        line_note = str(row.get('Line_Note', '')).strip() # Sometimes addresses might be in Line_Note
        
        for ls_address in self.LINE_SHOP_ADDRESSES:
            if ls_address.lower() in job_address.lower() or \
               ls_address.lower() in job_id.lower() or \
               ls_address.lower() in line_note.lower():
                issues.append(f"Line Shop Address Detected: '{ls_address}'")
                break # Flag once per record for this type of issue
        
        # 2. Validate Client Job # (Work Order)
        client_job_number_raw = str(row.get('client_job_number', '')).strip()
        
        # Try to extract the core work order number (e.g., DAP0413021) using the defined regex.
        wo_match = self.CORE_WORK_ORDER_REGEX.search(client_job_number_raw)
        extracted_core_wo = wo_match.group(1) if wo_match else None
        
        # Rule: "DAP projects with 7 digit numbers after the DAP are acceptable in all circumstances."
        # This means if a core WO is found, it's considered valid in format.
        # If no core WO is found AND the raw string is not empty or a placeholder, it's unparseable.
        if not extracted_core_wo:
            if client_job_number_raw and client_job_number_raw.upper() != 'SEE FOREMAN':
                issues.append(f"Unparseable Work Order Format: '{client_job_number_raw}'")
        
        # Rule: "non compliant should be if the Job# is more than 10 characters."
        # This applies to the *entire string* in the 'Client Job #' column.
        # If the extracted_core_wo is 10 chars (e.g., "DAP0413021"), and the raw string is also 10 chars, it's compliant.
        # If the raw string is > 10 chars (e.g., "DAP0413021-STORM"), it's non-compliant due to length.
        if len(client_job_number_raw) > 10:
            # We add this issue regardless of whether a core WO was extracted,
            # as the rule is about the total length of the raw string.
            issues.append(f"Work Order String Too Long (>10 characters): '{client_job_number_raw}'")
            
        # 3. Validate Contact Name
        contact_name = str(row.get('contact_name', '')).strip().upper()
        if contact_name in self.INVALID_CONTACT_NAMES:
            issues.append(f"Invalid Contact Name: '{contact_name}' (Cannot sign off)")
            
        return issues
    
    def _consolidate_split_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consolidates records that are pre-split into regular and overtime components (e.g., source has 1-MAN and 1-MAN OT for same shift).
        Also calculates duration for records with blank reported hours.
        """
        print("Consolidating pre-split and blank-hour records...")
        
        consolidated_records = []
        
        # Define columns for grouping a unique time block (excluding reported_hours, item_number, and validation_issues)
        # 'validation_issues' is specifically excluded here as it's a list (unhashable) and is carried over from base_record.
        group_cols_for_consolidation = [
            'employee_name', 'job_id', 'start_datetime', 'end_datetime', 
            'lunch_deduction', 'is_call_out', 'client_job_number', 
            'contact_name', 'bill_to_account', 'job_address', 'Line_Note' # Include Line_Note for grouping integrity
        ]
        
        # Sort for deterministic processing of records within a group (e.e., '1-MAN' before '1-MAN OT')
        # This ensures that group_df.iloc[0] is consistently the "main" record if multiple exist.
        df_sorted = df.sort_values(by=['employee_name', 'start_datetime', 'item_number']).copy()

        # Iterate through unique groups, processing each group's records.
        for _, group_df in df_sorted.groupby(group_cols_for_consolidation):
            # Take the first record of the group as the base for the consolidated record.
            # This ensures all non-time-related original data (including validation_issues) is carried forward.
            base_record = group_df.iloc[0].copy()

            regular_parts = []
            ot_parts = []
            empty_reported_hours_parts = []

            # Categorize the records within the current group.
            for _, record_row in group_df.iterrows():
                item_num = str(record_row.get('item_number', '')).strip()
                reported_hours_val = record_row.get('reported_hours')
                
                if pd.isna(reported_hours_val) or str(reported_hours_val).strip() == '':
                    empty_reported_hours_parts.append(record_row)
                elif 'OT' in item_num.upper():
                    ot_parts.append(record_row)
                else:
                    regular_parts.append(record_row)
            
            if (regular_parts or ot_parts) and not empty_reported_hours_parts:
                # Scenario: Record(s) were explicitly pre-split in source (e.g., 1-MAN and 1-MAN OT entries).
                total_regular_from_source = sum(np.nan_to_num(pd.to_numeric(r.get('reported_hours', 0), errors='coerce')) for r in regular_parts)
                total_ot_from_source = sum(np.nan_to_num(pd.to_numeric(r.get('reported_hours', 0), errors='coerce')) for r in ot_parts)
                
                base_record['reported_hours'] = total_regular_from_source + total_ot_from_source
                base_record['pre_split_regular'] = total_regular_from_source
                base_record['pre_split_overtime'] = total_ot_from_source
                base_record['is_pre_split'] = True
                base_record['item_number'] = '1-MAN (PRE-SPLIT)' # Update item number for clarity in output
                
                self.calculation_log.append(
                    f"Consolidated {base_record['employee_name']} {base_record['job_id']}: "
                    f"{len(group_df)} records -> Pre-split Regular: {total_regular_from_source:.2f}h, Pre-split OT: {total_ot_from_source:.2f}h"
                )
            elif empty_reported_hours_parts:
                # Scenario: 'reported_hours' was blank in the source, so calculate from times.
                # Use the first record from the empty_reported_hours_parts for time details.
                record_for_duration_calc = empty_reported_hours_parts[0]
                duration_calculated = self.calculate_duration(
                    record_for_duration_calc['start_datetime'],
                    record_for_duration_calc['end_datetime'],
                    record_for_duration_calc['lunch_deduction']
                )
                
                base_record['reported_hours'] = duration_calculated
                base_record['is_pre_split'] = False # Not pre-split, as hours were calculated
                base_record['item_number'] = record_for_duration_calc.get('item_number', '1-MAN') # Keep original or default
                
                self.calculation_log.append(
                    f"Filled blank hours for {base_record['employee_name']} {base_record['job_id']}: Calculated {duration_calculated:.2f}h"
                )
            else:
                # Fallback: This case should ideally not be hit if data is well-formed
                # and logic correctly handles pre-splits or blank hours.
                base_record['is_pre_split'] = False 
                self.calculation_log.append(
                    f"Fallback consolidation for {base_record['employee_name']} {base_record['job_id']}: Processed {len(group_df)} records, taking first as is."
                )
            
            consolidated_records.append(base_record)
        
        result_df = pd.DataFrame(consolidated_records)
        print(f"Consolidated {len(df)} raw records into {len(result_df)} consolidated records.")
        return result_df
    
    def calculate_duration(self, start_time: datetime, end_time: datetime, 
                          lunch_deduction: float = 0.0) -> float:
        """Calculate job duration in hours, applying lunch deduction."""
        if pd.isna(start_time) or pd.isna(end_time):
            return 0.0
        
        duration = (end_time - start_time).total_seconds() / 3600.0
        duration -= lunch_deduction # Apply deduction
        
        return max(0.0, duration) # Ensure non-negative duration
    
    def apply_rounding_rules(self, hours: float) -> float:
        """Apply AEP time rounding rules to the nearest quarter hour."""
        if pd.isna(hours) or hours <= 0:
            return 0.0
        
        whole_hours = int(hours)
        minutes = (hours - whole_hours) * 60
        
        # Rounding logic:
        # 0-6 mins -> .00
        # 7-21 mins -> .15 (0.25 hours)
        # 22-36 mins -> .30 (0.50 hours)
        # 37-51 mins -> .45 (0.75 hours)
        # 52-59 mins -> .00 and increment hour (rounds up to next whole hour)
        if 0 <= minutes <= 6:
            rounded_minutes = 0
        elif 7 <= minutes <= 21:
            rounded_minutes = 15  # 0.25 hours
        elif 22 <= minutes <= 36:
            rounded_minutes = 30  # 0.50 hours
        elif 37 <= minutes <= 51:
            rounded_minutes = 45  # 0.75 hours
        else: # 52-59 minutes
            rounded_minutes = 0
            whole_hours += 1 # Round up to next whole hour
        
        return whole_hours + (rounded_minutes / 60.0)
    
    def detect_overlaps(self, employee_jobs: pd.DataFrame) -> List[Dict]:
        """Detect overlapping time entries for an employee.
        An overlap is flagged if one job starts before the previous one ends,
        AND they are different jobs.
        """
        overlaps = []
        # Sort jobs chronologically for accurate overlap detection
        jobs = employee_jobs.sort_values('start_datetime').copy()
        
        for i in range(len(jobs) - 1):
            current_job = jobs.iloc[i]
            next_job = jobs.iloc[i + 1]
            
            # Overlap occurs if current job's end time is strictly after next job's start time
            # AND the jobs are distinct (different job_ids)
            if current_job['end_datetime'] > next_job['start_datetime'] and \
               current_job['job_id'] != next_job['job_id']:
                
                # Calculate the actual overlap duration
                overlap_duration_td = current_job['end_datetime'] - next_job['start_datetime']
                overlap_duration_hours = overlap_duration_td.total_seconds() / 3600.0

                overlap_info = {
                    'job1_id': current_job['job_id'],
                    'job1_time': f"{current_job['start_datetime'].strftime('%Y-%m-%d %H:%M')} - {current_job['end_datetime'].strftime('%H:%M')}",
                    'job2_id': next_job['job_id'],
                    'job2_time': f"{next_job['start_datetime'].strftime('%Y-%m-%d %H:%M')} - {next_job['end_datetime'].strftime('%H:%M')}",
                    'overlap_duration': overlap_duration_hours
                }
                overlaps.append(overlap_info)
        
        return overlaps
    
    def process_employee_week(self, employee_name: str, week_data: pd.DataFrame) -> List[Dict]:
        """
        Processes one employee's weekly data, applying AEP overtime rules.
        Rules applied in order:
        1. Overlap Detection (hours zeroed for overlapping jobs)
        2. Initial Hours (from pre-split data or raw duration)
        3. Sunday Work / Call Out Overrides (always OT, highest priority)
        4. Daily >10 Hours Rule
        5. Weekly >40 Hours Rule
        """
        results = []
        
        # Ensure jobs are sorted chronologically for rule application logic (especially overlaps)
        week_data = week_data.sort_values(['work_date', 'start_datetime']).copy()
        
        # Step 1: Detect overlaps and identify jobs involved.
        # Overlapping jobs will have their hours zeroed out (not counted for payment).
        overlaps = self.detect_overlaps(week_data)
        overlapping_job_ids = set()
        if overlaps:
            for overlap_detail in overlaps:
                # Add both jobs involved in the overlap to the set for zeroing hours
                overlapping_job_ids.add(overlap_detail['job1_id'])
                overlapping_job_ids.add(overlap_detail['job2_id'])
                # Log overlap details in validation_exceptions for reporting.
                # This ensures they are flagged as exceptions regardless of how hours are calculated.
                self.validation_exceptions.append({
                    'employee_name': employee_name,
                    'job_id': f"{overlap_detail['job1_id']} & {overlap_detail['job2_id']}",
                    'client_job_number': 'N/A', # Specific client job numbers are for individual job entries
                    'contact_name': 'N/A',
                    'job_address': 'N/A',
                    'issues': f"Overlapping Shifts: {overlap_detail['job1_time']} overlaps with {overlap_detail['job2_time']} by {overlap_detail['overlap_duration']:.2f}h"
                })
                self.calculation_log.append(f"OVERLAP DETECTED for {employee_name}: {overlap_detail}")
        
        # Process each job in the week
        for idx, job in week_data.iterrows():
            is_pre_split = job.get('is_pre_split', False)
            
            # Initialize job result structure
            # Ensure all relevant original columns are included for context in final output.
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
                'ot_reasons': [], # List to store all applicable OT reasons
                'regular_hours': 0.0, # Initialized to 0.0, will be assigned below
                'overtime_hours': 0.0, # Initialized to 0.0, will be assigned below
                'calculation_notes': '', # Notes about how hours were calculated
                'validation_issues': job.get('validation_issues', []), # Carry over issues found in _clean_data
                'client_job_number': job.get('client_job_number', ''), # Include original data for context in final output
                'bill_to_account': job.get('bill_to_account', ''),
                'contact_name': job.get('contact_name', ''),
                'job_address': job.get('job_address', ''),
                'Line_Note': job.get('Line_Note', '') # Include Line_Note for context
            }

            # Step 1.1: NEW RULE: If job is part of an overlapping set, zero out its hours immediately.
            # This is a high-priority rule based on Brittany's feedback ("you don't calculate it in").
            if job['job_id'] in overlapping_job_ids:
                job_result['overlap_status'] = 'Overlap'
                job_result['regular_hours'] = 0.0
                job_result['overtime_hours'] = 0.0
                job_result['calculation_notes'] = "Hours set to 0.0 due to overlapping shifts."
                results.append(job_result)
                continue # Skip remaining hour calculation for this job and move to next
            else:
                job_result['overlap_status'] = '' # No overlap for this specific job record

            # Step 2: Determine initial hours (for non-overlapping jobs).
            # This is the starting point for calculating regular vs. overtime hours.
            if is_pre_split:
                # If source data provided pre-split regular and OT hours, use those.
                pre_regular = float(job.get('pre_split_regular', 0))
                pre_ot = float(job.get('pre_split_overtime', 0))
                total_duration_from_source = pre_regular + pre_ot
                
                # Apply rounding to the pre-split components
                rounded_regular = self.apply_rounding_rules(pre_regular)
                rounded_ot = self.apply_rounding_rules(pre_ot)
                rounded_total = rounded_regular + rounded_ot # Sum of rounded components

                job_result.update({
                    'raw_duration': total_duration_from_source,
                    'rounded_duration': rounded_total,
                    'regular_hours': rounded_regular,
                    'overtime_hours': rounded_ot,
                    'ot_reasons': ['Pre-Split in Source Data'] if rounded_ot > 0 else [],
                    'calculation_notes': f"Pre-split: {pre_regular:.2f}h reg + {pre_ot:.2f}h OT -> {rounded_regular:.2f}h reg + {rounded_ot:.2f}h OT",
                })
            else:
                # If not pre-split, calculate raw duration from start/end times and apply rounding.
                raw_duration = self.calculate_duration(
                    job['start_datetime'], 
                    job['end_datetime'], 
                    job['lunch_deduction']
                )
                rounded_duration = self.apply_rounding_rules(raw_duration)
                
                job_result.update({
                    'raw_duration': raw_duration,
                    'rounded_duration': rounded_duration,
                    'regular_hours': rounded_duration, # Initially assume all are regular hours
                    'overtime_hours': 0.0,
                    'calculation_notes': f"Raw: {raw_duration:.2f}h, Rounded: {rounded_duration:.2f}h" + (f", Lunch: {job['lunch_deduction']:.2f}h" if job['lunch_deduction'] > 0 else ""),
                })
            
            # Step 3: Apply Sunday Work and Call Out rules as absolute overrides.
            # These rules take precedence and convert ALL hours for the specific job to overtime,
            # regardless of whether they were pre-split or initially regular.
            
            # Rule: Sunday work is always overtime.
            if job_result['day_of_week'] == 'Sunday':
                # Convert all regular hours (if any) for this job to overtime
                if job_result['regular_hours'] > 0:
                    job_result['overtime_hours'] += job_result['regular_hours']
                    job_result['regular_hours'] = 0.0
                # Add 'Sunday Work' as a reason, even if some hours were already overtime from pre-split
                if 'Sunday Work' not in job_result['ot_reasons']:
                    job_result['ot_reasons'].append('Sunday Work')
                
            # Rule: Call-out work is always overtime (only if not already classified as Sunday work).
            elif job_result['is_call_out']:
                # Convert all regular hours (if any) for this job to overtime
                if job_result['regular_hours'] > 0:
                    job_result['overtime_hours'] += job_result['regular_hours']
                    job_result['regular_hours'] = 0.0
                # Add 'Call Out' as a reason, even if some hours were already overtime from pre-split
                if 'Call Out' not in job_result['ot_reasons']:
                    job_result['ot_reasons'].append('Call Out')

            results.append(job_result)
        
        # Step 4: Apply daily >10 hour rule.
        # This rule applies only to hours that are *not* already classified as OT by Sunday/Call-Out rules.
        results = self._apply_daily_over_10_rule(results)
        
        # Step 5: Apply weekly >40 hour rule.
        # This rule converts regular hours to OT if the weekly total exceeds 40, again
        # respecting prior Sunday/Call-Out classifications.
        results = self._apply_weekly_over_40_rule(results)
        
        # Final calculations for the week summary
        # Sums reflect hours after all rules, including any zeroed hours for overlaps.
        final_regular_total = sum(j['regular_hours'] for j in results)
        final_ot_total = sum(j['overtime_hours'] for j in results)
        
        # Add week summary totals to each job record for convenience in output.
        for job in results:
            job['week_regular_total'] = final_regular_total
            job['week_ot_total'] = final_ot_total
        
        # Final compliance check: Ensure no employee has >40 regular hours after all rules.
        # This should ideally be 0 if the weekly >40 rule worked perfectly.
        if final_regular_total > 40.01: # Use a small tolerance for floating point numbers
            self.calculation_log.append(
                f"ERROR: {employee_name} has {final_regular_total:.2f} regular hours (over 40) after all rules! Review calculations."
            )
        
        return results
    
    def _apply_daily_over_10_rule(self, jobs: List[Dict]) -> List[Dict]:
        """
        Applies the rule: any hours worked over 10 in a single day are overtime.
        This rule applies only to jobs that are not already fully classified as OT by Sunday/Call-Out rules,
        and not to jobs with zeroed hours due to overlaps.
        """
        daily_groups = {}
        for job in jobs:
            date = job['work_date']
            if date not in daily_groups:
                daily_groups[date] = []
            daily_groups[date].append(job)
        
        for date, day_jobs in daily_groups.items():
            # Filter for jobs eligible for daily OT conversion.
            # They must still have regular hours, not be Sunday/Call-Out OT, and not be overlapping jobs.
            eligible_jobs = [job for job in day_jobs 
                           if job['regular_hours'] > 0 
                           and 'Sunday Work' not in job['ot_reasons']
                           and 'Call Out' not in job['ot_reasons']
                           and job.get('overlap_status') != 'Overlap'
                           ]
            
            total_regular_day = sum(job['regular_hours'] for job in eligible_jobs)
            
            if total_regular_day > 10:
                excess_to_convert = total_regular_day - 10
                
                # Convert regular hours to OT, starting from the latest jobs of the day.
                # Sort in reverse chronological order.
                day_jobs_sorted_reverse = sorted(eligible_jobs, key=lambda x: x['start_time'], reverse=True)
                
                remaining_excess = excess_to_convert
                for job in day_jobs_sorted_reverse:
                    if job['regular_hours'] > 0 and remaining_excess > 0:
                        convertible_from_this_job = min(job['regular_hours'], remaining_excess)
                        
                        job['regular_hours'] -= convertible_from_this_job
                        job['overtime_hours'] += convertible_from_this_job
                        if 'Over 10 Hours/Day' not in job['ot_reasons']:
                            job['ot_reasons'].append('Over 10 Hours/Day')
                        
                        remaining_excess -= convertible_from_this_job
                        
                        if remaining_excess <= 0:
                            break # All excess converted
        
        return jobs
    
    def _apply_weekly_over_40_rule(self, jobs: List[Dict]) -> List[Dict]:
        """
        Applies the rule: total regular hours for the week cannot exceed 40.
        Any hours over 40 (after Sunday/Call-Out/Daily >10 rules) are converted to overtime.
        This rule applies only to hours that are not already fully classified as OT by Sunday/Call-Out rules,
        and not to jobs with zeroed hours due to overlaps.
        """
        # Identify all jobs that are eligible to contribute to the 40-hour regular work week.
        # These are jobs that still have regular hours and are not already Sunday/Call-Out OT or overlapping.
        eligible_for_conversion_jobs = [job for job in jobs
                                        if job['regular_hours'] > 0
                                        and 'Sunday Work' not in job['ot_reasons']
                                        and 'Call Out' not in job['ot_reasons']
                                        and job.get('overlap_status') != 'Overlap'
                                       ]
        
        total_regular_hours_eligible = sum(job['regular_hours'] for job in eligible_for_conversion_jobs)
        
        if total_regular_hours_eligible <= 40:
            return jobs # No excess regular hours this week, so no conversion needed.
        
        # If total_regular_hours_eligible is > 40, calculate the excess that needs to become overtime.
        excess_to_convert = total_regular_hours_eligible - 40
        
        # Convert regular hours to OT, starting from the latest jobs of the week.
        # Sort eligible jobs in reverse chronological order (latest jobs first).
        jobs_sorted_reverse = sorted(eligible_for_conversion_jobs, 
                                     key=lambda x: (x['work_date'], x['start_time']), reverse=True)
        
        remaining_excess = excess_to_convert
        converted_job_ids = set() # Track unique jobs where conversion happened for logging
        
        for job in jobs_sorted_reverse:
            if remaining_excess <= 0:
                break # All excess converted
                
            if job['regular_hours'] > 0:
                convertible_from_this_job = min(job['regular_hours'], remaining_excess)
                
                job['regular_hours'] -= convertible_from_this_job
                job['overtime_hours'] += convertible_from_this_job
                if 'Over 40 Hours/Week' not in job['ot_reasons']:
                    job['ot_reasons'].append('Over 40 Hours/Week')
                
                remaining_excess -= convertible_from_this_job
                converted_job_ids.add(job['job_id'])
                
                # Update calculation notes (append to existing notes)
                if job['calculation_notes']:
                    job['calculation_notes'] += f" | Converted {convertible_from_this_job:.2f}h reg->OT (weekly >40)"
                else:
                    job['calculation_notes'] = f"Converted {convertible_from_this_job:.2f}h reg->OT (weekly >40)"
        
        if converted_job_ids:
            self.calculation_log.append(
                f"Weekly >40 rule applied: Converted {excess_to_convert:.2f}h regular->OT for {len(converted_job_ids)} jobs."
            )
        
        return jobs
    
    def process_all_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Orchestrates the processing of all employee data.
        Groups data by employee and then by week to apply rules sequentially.
        """
        all_results = []
        
        # Iterate through each unique employee
        for employee_name in df['employee_name'].unique():
            employee_data = df[df['employee_name'] == employee_name]
            
            # Then, for each employee, iterate through their unique work weeks
            for week_start_date in employee_data['week_start'].unique():
                week_data = employee_data[employee_data['week_start'] == week_start_date]
                
                print(f"Processing {employee_name} - Week of {week_start_date}...")
                week_results = self.process_employee_week(employee_name, week_data)
                all_results.extend(week_results)
        
        # Convert the list of dictionaries (each representing a job result) into a DataFrame
        results_df = pd.DataFrame(all_results)
        
        # Create a DataFrame for records filtered due to missing core data during initial parsing
        filtered_df = pd.DataFrame(self.filtered_data) if self.filtered_data else pd.DataFrame()
        
        return results_df, filtered_df
    
    def _create_summary(self, results_df: pd.DataFrame) -> List[Dict]:
        """
        Generates summary statistics for each employee, including compliance status.
        The compliance status now factors in both hour-based rules and data validation exceptions.
        """
        summary = []
        
        # NEW: Identify employees who had any validation exceptions.
        employees_with_validation_exceptions = set()
        for exception_entry in self.validation_exceptions:
            employees_with_validation_exceptions.add(exception_entry['employee_name'])

        for employee_name in results_df['employee_name'].unique():
            emp_data = results_df[results_df['employee_name'] == employee_name]
            
            # Count OT reasons for this employee across all their jobs
            ot_reason_counts = {}
            for reasons_list in emp_data['ot_reasons']:
                for reason in reasons_list:
                    ot_reason_counts[reason] = ot_reason_counts.get(reason, 0) + 1
            
            # Calculate per-week regular and overtime totals for this employee
            week_groups = emp_data.groupby('week_start')
            weekly_regular_hours = []
            weekly_overtime_hours = []
            
            for week_start_date, week_data in week_groups:
                week_regular = week_data['regular_hours'].sum()
                week_ot = week_data['overtime_hours'].sum()
                weekly_regular_hours.append(week_regular)
                weekly_overtime_hours.append(week_ot)
            
            # Calculate overall totals and maximums for summary display
            total_regular_hours_cumulative = sum(weekly_regular_hours)
            total_overtime_hours_cumulative = sum(weekly_overtime_hours)
            total_hours_cumulative = total_regular_hours_cumulative + total_overtime_hours_cumulative
            
            # Key compliance metric: the maximum regular hours recorded in any single week
            max_weekly_regular = max(weekly_regular_hours) if weekly_regular_hours else 0.0
            max_weekly_overtime = max(weekly_overtime_hours) if weekly_overtime_hours else 0.0
            
            # Determine overall compliance status for the employee.
            is_compliant = True
            
            # Condition B: Non-compliant if max weekly regular hours are over 40 (after all rules)
            if max_weekly_regular > 40.01: # Small tolerance for floating point numbers
                is_compliant = False
            
            # Condition C: NEW: Non-compliant if the employee has any validation exceptions (Pat Denny, bad WO, etc.)
            if employee_name in employees_with_validation_exceptions:
                is_compliant = False # Mark as non-compliant due to data quality issues
            
            summary.append({
                'employee_name': employee_name,
                'total_regular_hours': total_regular_hours_cumulative,
                'total_overtime_hours': total_overtime_hours_cumulative,
                'total_hours': total_hours_cumulative,
                'max_weekly_regular': max_weekly_regular, # Key compliance metric for 40h rule
                'max_weekly_overtime': max_weekly_overtime,
                'weeks_worked': len(weekly_regular_hours),
                'jobs_processed': len(emp_data),
                'call_out_jobs': emp_data['is_call_out'].sum(),
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
        """
        Exports results to an Excel file with multiple sheets:
        - 'final_data': Detailed processed job records with all calculations and flags.
        - 'filtered_records': Jobs removed due to missing/invalid core data.
        - 'employee_summary': High-level summary per employee.
        - 'validation_exceptions': Detailed list of all jobs with specific validation issues.
        - 'detailed_report': A comprehensive text-based overview.
        """
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Prepare main results DataFrame for export
            export_df = results_df.copy()
            # Convert lists of reasons/issues to comma-separated strings for display
            export_df['ot_reasons_text'] = export_df['ot_reasons'].apply(lambda x: ', '.join(x) if x else '')
            export_df['validation_issues_text'] = export_df['validation_issues'].apply(lambda x: ', '.join(x) if x else '')
            
            # Define desired column order for 'final_data' sheet.
            # 'validation_issues_text' placed before hours as requested (approx. P, Q, R columns).
            column_order = [
                'employee_name', 'work_date', 'day_of_week', 'week_start', 'job_id',
                'start_time', 'end_time', 'raw_duration', 'rounded_duration',
                'ot_reasons_text', 'is_call_out', 'lunch_deduction', 'is_pre_split', 'overlap_status',
                'client_job_number', 'bill_to_account', 'contact_name', 'job_address', 'Line_Note', # Grouped source columns for context
                'validation_issues_text', # This is the requested "Issues" column (approx. P)
                'regular_hours',          # Regular hours (approx. Q)
                'overtime_hours',         # Overtime hours (approx. R)
                'calculation_notes',      # Detailed notes about calculation
                'week_regular_total', 'week_ot_total' # Weekly totals repeated per job for context
            ]
            
            # Filter column_order to include only columns that actually exist in the DataFrame.
            available_columns = [col for col in column_order if col in export_df.columns]
            export_df = export_df[available_columns]
            
            export_df.to_excel(writer, sheet_name='final_data', index=False)
            
            # Export records filtered during initial cleaning (e.g., missing timestamps)
            if not filtered_df.empty:
                filtered_df.to_excel(writer, sheet_name='filtered_records', index=False)
            
            # Export employee summary
            summary_data = self._create_summary(results_df)
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='employee_summary', index=False)
            
            # Export detailed validation exceptions.
            # This sheet will list all jobs that triggered any validation rule.
            if self.validation_exceptions:
                validation_exceptions_df = pd.DataFrame(self.validation_exceptions)
                validation_exceptions_df = validation_exceptions_df.sort_values(
                    by=['employee_name', 'job_id', 'issues'] # Sort for readability
                )
                validation_exceptions_df.to_excel(writer, sheet_name='validation_exceptions', index=False)
            
            # Export comprehensive text report
            report_text = self.generate_detailed_report(results_df, filtered_df)
            report_df = pd.DataFrame({'Report': [report_text]})
            report_df.to_excel(writer, sheet_name='detailed_report', index=False)
        
        print(f"Results exported to {output_file}.")
        return output_file
    
    def generate_detailed_report(self, results_df: pd.DataFrame, filtered_df: pd.DataFrame) -> str:
        """Generates a detailed text-based report summarizing processing and validation."""
        report = []
        report.append("AEP OVERTIME CALCULATION REPORT")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Overall Summary
        total_employees = results_df['employee_name'].nunique()
        total_jobs_processed = len(results_df)
        total_regular_hours = results_df['regular_hours'].sum()
        total_overtime_hours = results_df['overtime_hours'].sum()
        
        report.append("OVERALL SUMMARY")
        report.append("-" * 20)
        report.append(f"Total Unique Employees Processed: {total_employees}")
        report.append(f"Total Jobs Processed (after initial filtering/deduplication): {total_jobs_processed}")
        report.append(f"Total Calculated Regular Hours: {total_regular_hours:.2f}")
        report.append(f"Total Calculated Overtime Hours: {total_overtime_hours:.2f}")
        report.append(f"Grand Total Hours: {(total_regular_hours + total_overtime_hours):.2f}")
        report.append("")
        
        # Compliance Check: Weekly Regular Hours Limit (40 hours)
        emp_summary_for_compliance_check = results_df.groupby(['employee_name', 'week_start'])['regular_hours'].sum().reset_index()
        over_40_weeks = emp_summary_for_compliance_check[emp_summary_for_compliance_check['regular_hours'] > 40.01]
        
        if len(over_40_weeks) > 0:
            report.append("⚠️  WEEKS WITH >40 REGULAR HOURS (Error/Warning - Review Employees' Weekly Totals)")
            report.append("-" * 60)
            for _, week in over_40_weeks.iterrows():
                report.append(f"  - {week['employee_name']} - Week starting {week['week_start']}: {week['regular_hours']:.2f}h regular")
            report.append("")
        else:
            report.append("✅ COMPLIANCE CHECK: No individual weeks exceed 40 regular hours for any employee.")
            report.append("")
        
        # Overtime Breakdown by Reason
        ot_reasons_counts = {}
        for reasons_list in results_df['ot_reasons']:
            for reason in reasons_list:
                ot_reasons_counts[reason] = ot_reasons_counts.get(reason, 0) + 1
        
        if ot_reasons_counts:
            report.append("OVERTIME BREAKDOWN (by reason)")
            report.append("-" * 30)
            for reason, count in sorted(ot_reasons_counts.items()):
                # Calculate total hours for each reason based on jobs where that reason is present
                total_hours_for_reason = results_df[results_df['ot_reasons'].apply(lambda x: reason in x)]['overtime_hours'].sum()
                report.append(f"  - {reason}: {count} jobs, {total_hours_for_reason:.2f} hours")
            report.append("")
        
        # NEW: Job Validation Exceptions Summary
        if self.validation_exceptions:
            report.append("🔴 JOB DATA VALIDATION EXCEPTIONS FOUND")
            report.append("-" * 40)
            exception_type_counts = {}
            for exc_entry in self.validation_exceptions:
                issues_list = exc_entry['issues'].split(', ') # Split the string of issues
                for issue_type in issues_list:
                    exception_type_counts[issue_type] = exception_type_counts.get(issue_type, 0) + 1
            
            for issue_type, count in sorted(exception_type_counts.items()):
                report.append(f"  - {issue_type}: {count} jobs")
            report.append(f"\nTotal unique jobs with validation exceptions: {len(set(exc['job_id'] for exc in self.validation_exceptions))}")
            report.append(f"See 'validation_exceptions' sheet in Excel for detailed list of {len(self.validation_exceptions)} flagged records.")
            report.append("")
        else:
            report.append("✅ VALIDATION: No specific job data validation exceptions found.")
            report.append("")
            
        # Summary of records filtered during initial parsing (e.g., missing required data)
        if not filtered_df.empty:
            report.append("RECORDS FILTERED OUT DURING INITIAL DATA LOADING")
            report.append("-" * 50)
            filter_reason_counts = filtered_df['filter_reason'].value_counts()
            for reason, count in filter_reason_counts.items():
                report.append(f"  - {reason}: {count} records")
            report.append("")
        
        return "\n".join(report)

# Flask Routes (these remain unchanged as they interact with the Calculator class)
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            flash('No file selected. Please choose a file to upload.')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected. Please choose a file to upload.')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            # Use a temporary directory for file storage during processing
            temp_dir = tempfile.mkdtemp()
            input_file_path = os.path.join(temp_dir, filename)
            file.save(input_file_path)
            
            calculator = AEPOvertimeCalculator()
            
            df = calculator.parse_input_file(input_file_path)
            
            # Check if any data remains after initial cleaning
            if df.empty:
                flash('No valid data found in the uploaded file after initial cleaning. Please check your file format and content.')
                return redirect(url_for('index'))

            results_df, filtered_df = calculator.process_all_data(df)
            
            output_filename_base = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"aep_overtime_results_{output_filename_base}.xlsx"
            output_file_path = os.path.join(temp_dir, output_filename)
            
            # Export results to the Excel file
            calculator.export_results(results_df, filtered_df, output_file_path)
            
            # Prepare summary data for display on the webpage
            summary_data_for_display = calculator._create_summary(results_df)
            detailed_report_for_display = calculator.generate_detailed_report(results_df, filtered_df)
            
            # Calculate overall key metrics for the main summary cards
            total_employees = results_df['employee_name'].nunique()
            total_jobs_processed = len(results_df) # Total jobs after cleaning/deduplication
            total_regular_sum = results_df['regular_hours'].sum()
            total_overtime_sum = results_df['overtime_hours'].sum()
            
            # Determine overall compliance status for the webpage display
            emp_summary_check = results_df.groupby(['employee_name', 'week_start'])['regular_hours'].sum().reset_index()
            # Count weeks where regular hours exceed 40 (even slightly)
            over_40_weeks_count = (emp_summary_check['regular_hours'] > 40.01).sum()
            
            # Overall system compliance is non-compliant if any employee has >40 regular hours or if any validation exceptions occurred.
            system_compliance_status = "COMPLIANT" if over_40_weeks_count == 0 and not calculator.validation_exceptions else "NON-COMPLIANT"
            
            # Store file path in session for subsequent download request
            from flask import session
            session['output_file'] = output_file_path
            session['output_filename'] = output_filename
            
            return render_template('results.html', 
                                 summary={
                                     'total_employees': total_employees,
                                     'total_jobs': total_jobs_processed,
                                     'total_regular': total_regular_sum,
                                     'total_ot': total_overtime_sum,
                                     'total_hours': total_regular_sum + total_overtime_sum,
                                     'compliance_status': system_compliance_status,
                                     'over_40_weeks': over_40_weeks_count,
                                     'validation_issues_found': len(calculator.validation_exceptions) > 0 # Flag for UI to show warning badge
                                 },
                                 employee_summary=summary_data_for_display[:10], # Show top 10 employees in table
                                 detailed_report=detailed_report_for_display,
                                 download_filename=output_filename)
            
        else:
            flash('Invalid file type. Please upload CSV, XLS, or XLSX files only.')
            return redirect(request.url)
            
    except Exception as e:
        flash(f'An unexpected error occurred during file processing: {str(e)}. Please check the server logs for details.')
        print(f"Error in upload_file: {e}")
        traceback.print_exc() # Print full traceback to console/logs
        return redirect(url_for('index'))

@app.route('/download')
def download_file():
    try:
        from flask import session
        if 'output_file' not in session:
            flash('No file available for download. Please upload a file first.')
            return redirect(url_for('index'))
        
        output_file_path = session['output_file']
        output_filename = session['output_filename']
        
        if not os.path.exists(output_file_path):
            flash('File not found. It might have been deleted or expired. Please process the file again.')
            return redirect(url_for('index'))
        
        return send_file(output_file_path, 
                        as_attachment=True, 
                        download_name=output_filename,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    except Exception as e:
        flash(f'An error occurred during file download: {str(e)}')
        print(f"Error in download_file: {e}")
        traceback.print_exc()
        return redirect(url_for('index'))

# HTML Templates (These are embedded for self-contained script; usually in a 'templates' folder)
# These routes simply return the HTML string content for development purposes.
@app.route('/get_template/<template_name>')
def get_template(template_name):
    """Endpoint to serve template content for development. In production, these are static files."""
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
                        <li>Duplicate job entries (one instance kept, others flagged)</li>
                        <li>Overlapping shifts (hours set to zero, flagged)</li>
                        <li>Work order format/length (e.g., `DAPXXXXXXX` is ok, but `DAPXXXXXXX-NOTES` is flagged for length)</li>
                        <li>Line shop addresses used as work locations (flagged)</li>
                        <li>Invalid contact names for sign-off (flagged)</li>
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
                    <span class="badge bg-warning compliance-badge">! Data Exceptions</span>
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
    print("Open your web browser and go to: http://localhost:4444")
    print("Upload your timesheet file to process overtime calculations")
    
    app.run(debug=True, host='0.0.0.0', port=4444)