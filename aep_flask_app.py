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
                except:
                    # If that fails, try reading as HTML table (like the sample file)
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
            'Start': 'original_start',
            'Call Out': 'is_call_out',
            'Lunch Deduction': 'lunch_deduction',
            'Job Name': 'job_id',
            'Quantity': 'reported_hours',
            'Item Number': 'item_number',
            'Client Job #': 'client_job_number'
        }
        
        # Rename columns
        df_renamed = df.rename(columns=column_mapping)
        
        # Add missing columns with defaults
        required_columns = ['employee_name', 'start_time', 'end_time', 'is_call_out', 
                          'lunch_deduction', 'job_id', 'reported_hours', 'item_number']
        
        for col in required_columns:
            if col not in df_renamed.columns:
                if col == 'employee_name':
                    df_renamed[col] = ''
                elif col in ['reported_hours', 'lunch_deduction']:
                    df_renamed[col] = 0.0
                else:
                    df_renamed[col] = ''
        
        # Clean and convert data types
        df_renamed = self._clean_data(df_renamed)
        
        return df_renamed
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data"""
        # Remove rows with missing essential data
        initial_count = len(df)
        df = df.dropna(subset=['employee_name', 'start_time', 'end_time'])
        df = df[df['employee_name'].str.strip() != '']
        
        if len(df) < initial_count:
            filtered_count = initial_count - len(df)
            print(f"Filtered out {filtered_count} records with missing essential data")
        
        # Convert call out to boolean
        df['is_call_out'] = df['is_call_out'].astype(str).str.strip()
        df['is_call_out'] = df['is_call_out'].isin(['1', 'True', 'true', 'YES', 'yes'])
        
        # Convert lunch deduction to float and handle negative values
        df['lunch_deduction'] = pd.to_numeric(df['lunch_deduction'], errors='coerce').fillna(0.0)
        
        # CRITICAL FIX: Handle negative lunch deduction values
        # Negative values in source data appear to be data errors - treat as positive
        original_negative_count = (df['lunch_deduction'] < 0).sum()
        if original_negative_count > 0:
            print(f"Found {original_negative_count} records with negative lunch deduction - converting to positive")
            df['lunch_deduction'] = df['lunch_deduction'].abs()
        
        # Also validate lunch deduction is reasonable (typically 0.5 hours max)
        large_lunch_count = (df['lunch_deduction'] > 1.0).sum()
        if large_lunch_count > 0:
            print(f"Warning: {large_lunch_count} records have lunch deduction >1 hour")
            # Cap at 1 hour to prevent data errors
            df['lunch_deduction'] = df['lunch_deduction'].clip(upper=1.0)
        
        # Parse datetime columns
        df['start_datetime'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['end_datetime'] = pd.to_datetime(df['end_time'], errors='coerce')
        
        # Remove records with invalid dates
        valid_dates = df['start_datetime'].notna() & df['end_datetime'].notna()
        invalid_count = (~valid_dates).sum()
        if invalid_count > 0:
            print(f"Filtered out {invalid_count} records with invalid date/time data")
            # Log filtered records
            for idx, row in df[~valid_dates].iterrows():
                self.filtered_data.append({
                    'employee_name': row['employee_name'],
                    'job_id': row['job_id'],
                    'start_time': row['start_time'],
                    'end_time': row['end_time'],
                    'filter_reason': 'Invalid date/time format'
                })
        
        df = df[valid_dates].copy()
        
        # Handle pre-split records (records with same time but different quantities)
        df = self._consolidate_split_records(df)
        
        # Add derived columns
        df['work_date'] = df['start_datetime'].dt.date
        df['day_of_week'] = df['start_datetime'].dt.day_name()
        
        # CRITICAL FIX: Week should start on SUNDAY, not Monday
        # Calculate Sunday-to-Saturday weeks (AEP standard)
        df['week_start'] = df['start_datetime'].dt.date - pd.to_timedelta((df['start_datetime'].dt.dayofweek + 1) % 7, unit='D')

        return df
    
    def _consolidate_split_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Consolidate records that are pre-split into regular and overtime components"""
        print("Consolidating pre-split records...")
        
        # Group records by employee, job, start_time, and end_time
        group_cols = ['employee_name', 'job_id', 'start_datetime', 'end_datetime']
        grouped = df.groupby(group_cols)
        
        consolidated_records = []
        
        for group_key, group_df in grouped:
            if len(group_df) == 1:
                # Single record - keep as is but check if it needs quantity filled
                record = group_df.iloc[0].copy()
                
                # If quantity is missing, calculate from time duration
                if pd.isna(record.get('reported_hours')) or record.get('reported_hours') == '':
                    duration = self.calculate_duration(
                        record['start_datetime'],
                        record['end_datetime'],
                        record['lunch_deduction']
                    )
                    record['reported_hours'] = duration
                    record['item_number'] = '1-MAN'  # Default item type
                
                record['is_pre_split'] = False
                consolidated_records.append(record)
            else:
                # Multiple records for same time slot - consolidate them
                self._consolidate_group(group_df, consolidated_records)
        
        result_df = pd.DataFrame(consolidated_records)
        
        print(f"Consolidated {len(df)} records into {len(result_df)} records")
        return result_df
    
    def _consolidate_group(self, group_df: pd.DataFrame, consolidated_records: list):
        """Consolidate a group of records with the same time slot"""
        # Sort by item number to process in order
        group_df = group_df.sort_values(['item_number'], na_position='last')
        
        regular_records = []
        ot_records = []
        empty_records = []
        
        for _, record in group_df.iterrows():
            item_num = str(record.get('item_number', '')).strip()
            reported_hours = record.get('reported_hours', '')
            
            if pd.isna(reported_hours) or reported_hours == '':
                empty_records.append(record)
            elif 'OT' in item_num.upper():
                ot_records.append(record)
            else:
                regular_records.append(record)
        
        # If we have both regular and OT records, create a consolidated record
        if regular_records and ot_records:
            base_record = regular_records[0].copy()
            
            # Sum up the quantities
            total_regular = sum(float(r.get('reported_hours', 0)) for r in regular_records)
            total_ot = sum(float(r.get('reported_hours', 0)) for r in ot_records)
            
            # Create consolidated record with pre-split overtime info
            base_record['reported_hours'] = total_regular + total_ot
            base_record['pre_split_regular'] = total_regular
            base_record['pre_split_overtime'] = total_ot
            base_record['is_pre_split'] = True
            base_record['item_number'] = '1-MAN (PRE-SPLIT)'
            
            consolidated_records.append(base_record)
            
            # Log the consolidation
            duration = self.calculate_duration(
                base_record['start_datetime'],
                base_record['end_datetime'],
                base_record['lunch_deduction']
            )
            self.calculation_log.append(
                f"Consolidated {base_record['employee_name']} {base_record['job_id']}: "
                f"{len(group_df)} records -> Regular: {total_regular}h, OT: {total_ot}h, "
                f"Duration: {duration:.2f}h"
            )
            
        elif regular_records and not ot_records:
            # Only regular records - keep the first one
            record = regular_records[0].copy()
            record['is_pre_split'] = False
            consolidated_records.append(record)
            
        elif empty_records:
            # Only empty records - calculate duration and use first one
            record = empty_records[0].copy()
            duration = self.calculate_duration(
                record['start_datetime'],
                record['end_datetime'],
                record['lunch_deduction']
            )
            record['reported_hours'] = duration
            record['is_pre_split'] = False
            record['item_number'] = '1-MAN'
            consolidated_records.append(record)
        else:
            # Fallback - keep first record
            record = group_df.iloc[0].copy()
            record['is_pre_split'] = False
            consolidated_records.append(record)
    
    def calculate_duration(self, start_time: datetime, end_time: datetime, 
                          lunch_deduction: float = 0.0) -> float:
        """Calculate job duration with lunch deduction"""
        if pd.isna(start_time) or pd.isna(end_time):
            return 0.0
        
        # Calculate raw duration in hours
        duration = (end_time - start_time).total_seconds() / 3600.0
        
        # CRITICAL FIX: Apply lunch deduction correctly
        # Ensure lunch_deduction is positive and reasonable
        lunch_deduction = abs(float(lunch_deduction)) if not pd.isna(lunch_deduction) else 0.0
        lunch_deduction = min(lunch_deduction, 1.0)  # Cap at 1 hour max
        
        if lunch_deduction > 0:
            duration -= lunch_deduction
            
        # Ensure non-negative duration
        duration = max(0.0, duration)
        
        return duration
    
    def apply_rounding_rules(self, hours: float) -> float:
        """Apply AEP time rounding rules"""
        if pd.isna(hours) or hours <= 0:
            return 0.0
        
        # Split into hours and minutes
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
        else:  # 52-59 minutes
            rounded_minutes = 0
            whole_hours += 1
        
        return whole_hours + (rounded_minutes / 60.0)
    
    def detect_overlaps(self, employee_jobs: pd.DataFrame) -> List[Dict]:
        """Detect overlapping time entries for an employee"""
        overlaps = []
        jobs = employee_jobs.sort_values('start_datetime').copy()
        
        for i in range(len(jobs) - 1):
            current = jobs.iloc[i]
            next_job = jobs.iloc[i + 1]
            
            # Check if current job ends after next job starts
            if current['end_datetime'] > next_job['start_datetime']:
                overlap_info = {
                    'job1_id': current['job_id'],
                    'job1_time': f"{current['start_datetime']} - {current['end_datetime']}",
                    'job2_id': next_job['job_id'],
                    'job2_time': f"{next_job['start_datetime']} - {next_job['end_datetime']}",
                    'overlap_duration': (current['end_datetime'] - next_job['start_datetime']).total_seconds() / 3600.0
                }
                overlaps.append(overlap_info)
        
        return overlaps
    
    def process_employee_week(self, employee_name: str, week_data: pd.DataFrame) -> List[Dict]:
        """Process one employee's week of data applying all overtime rules"""
        results = []
        
        # Sort jobs chronologically
        week_data = week_data.sort_values(['work_date', 'start_datetime']).copy()
        
        # Check for overlaps
        overlaps = self.detect_overlaps(week_data)
        overlapping_job_ids = set()
        if overlaps:
            for overlap_detail in overlaps:
                overlapping_job_ids.add(overlap_detail['job1_id'])
                overlapping_job_ids.add(overlap_detail['job2_id'])
            # Keep original logging for the detailed report
            for overlap in overlaps:
                self.calculation_log.append(f"OVERLAP DETECTED for {employee_name}: {overlap}")
        
        # Process each job
        for idx, job in week_data.iterrows():
            # Check if this is a pre-split record
            is_pre_split = job.get('is_pre_split', False)
            
            if is_pre_split:
                # Job already has regular/OT split - use those values
                pre_regular = float(job.get('pre_split_regular', 0))
                pre_ot = float(job.get('pre_split_overtime', 0))
                total_duration = pre_regular + pre_ot
                
                # Apply rounding to the components
                rounded_regular = self.apply_rounding_rules(pre_regular)
                rounded_ot = self.apply_rounding_rules(pre_ot)
                rounded_total = rounded_regular + rounded_ot
                
                job_result = {
                    'employee_name': employee_name,
                    'job_id': job['job_id'],
                    'work_date': job['work_date'],
                    'day_of_week': job['day_of_week'],
                    'week_start': job['week_start'],
                    'start_time': job['start_datetime'],
                    'end_time': job['end_datetime'],
                    'raw_duration': total_duration,
                    'rounded_duration': rounded_total,
                    'lunch_deduction': job['lunch_deduction'],
                    'is_call_out': job['is_call_out'],
                    'regular_hours': rounded_regular,
                    'overtime_hours': rounded_ot,
                    'ot_reasons': ['Pre-Split in Source Data'] if rounded_ot > 0 else [],
                    'calculation_notes': f"Pre-split: {pre_regular:.2f}h reg + {pre_ot:.2f}h OT -> {rounded_regular:.2f}h reg + {rounded_ot:.2f}h OT",
                    'is_pre_split': True,
                    'overlap_status': 'Overlap' if job['job_id'] in overlapping_job_ids else ''
                }
                
            else:
                # Normal processing for non-pre-split records
                raw_duration = self.calculate_duration(
                    job['start_datetime'], 
                    job['end_datetime'], 
                    job['lunch_deduction']
                )
                
                rounded_duration = self.apply_rounding_rules(raw_duration)
                
                job_result = {
                    'employee_name': employee_name,
                    'job_id': job['job_id'],
                    'work_date': job['work_date'],
                    'day_of_week': job['day_of_week'],
                    'week_start': job['week_start'],
                    'start_time': job['start_datetime'],
                    'end_time': job['end_datetime'],
                    'raw_duration': raw_duration,
                    'rounded_duration': rounded_duration,
                    'lunch_deduction': job['lunch_deduction'],
                    'is_call_out': job['is_call_out'],
                    'regular_hours': 0.0,
                    'overtime_hours': 0.0,
                    'ot_reasons': [],
                    'calculation_notes': f"Raw: {raw_duration:.2f}h, Rounded: {rounded_duration:.2f}h" + (f", Lunch: {job['lunch_deduction']:.2f}h" if job['lunch_deduction'] > 0 else ""),
                    'is_pre_split': False,
                    'overlap_status': 'Overlap' if job['job_id'] in overlapping_job_ids else ''
                }
                
                # Apply initial overtime rules
                
                # Rule 1: Sunday work - all overtime (doesn't count toward 40h limit)
                if job['day_of_week'] == 'Sunday':
                    job_result['overtime_hours'] = rounded_duration
                    job_result['ot_reasons'].append('Sunday Work')
                    
                # Rule 2: Call out - all overtime (doesn't count toward 40h limit)
                elif job['is_call_out']:
                    job_result['overtime_hours'] = rounded_duration
                    job_result['ot_reasons'].append('Call Out')
                    
                # Rule 3: Regular time (will be adjusted later for daily >10 and weekly >40)
                else:
                    job_result['regular_hours'] = rounded_duration
            
            results.append(job_result)
        
        # Rule 4: Apply daily >10 hour rule (only to non-pre-split records)
        results = self._apply_daily_over_10_rule(results)
        
        # Rule 5: CRITICAL - Apply weekly >40 hour rule 
        results = self._apply_weekly_over_40_rule(results)
        
        # Add week summary to each job
        final_regular_total = sum(j['regular_hours'] for j in results)
        final_ot_total = sum(j['overtime_hours'] for j in results)
        
        for job in results:
            job['week_regular_total'] = final_regular_total
            job['week_ot_total'] = final_ot_total
        
        # Validation check - no employee should have >40 regular hours
        if final_regular_total > 40.01:  # Small tolerance for rounding
            self.calculation_log.append(
                f"ERROR: {employee_name} has {final_regular_total:.2f} regular hours (over 40)!"
            )
        
        return results
    
    def _apply_daily_over_10_rule(self, jobs: List[Dict]) -> List[Dict]:
        """Apply over 10 hours in a day rule (skip pre-split records)"""
        # Group by date
        daily_groups = {}
        for job in jobs:
            date = job['work_date']
            if date not in daily_groups:
                daily_groups[date] = []
            daily_groups[date].append(job)
        
        for date, day_jobs in daily_groups.items():
            # Calculate total regular hours for the day (excluding pre-split, Sunday, call-out)
            eligible_jobs = [job for job in day_jobs 
                           if not job.get('is_pre_split', False)
                           and 'Sunday Work' not in job['ot_reasons']
                           and 'Call Out' not in job['ot_reasons']]
            
            total_regular_day = sum(job['regular_hours'] for job in eligible_jobs)
            
            if total_regular_day > 10:
                excess = total_regular_day - 10
                
                # Apply excess to last job(s) of the day, working backward
                day_jobs_sorted = sorted(eligible_jobs, key=lambda x: x['start_time'], reverse=True)
                
                remaining_excess = excess
                for job in day_jobs_sorted:
                    if job['regular_hours'] > 0 and remaining_excess > 0:
                        # How much can we convert from this job?
                        convertible = min(job['regular_hours'], remaining_excess)
                        
                        # Convert regular to OT
                        job['regular_hours'] -= convertible
                        job['overtime_hours'] += convertible
                        job['ot_reasons'].append('Over 10 Hours/Day')
                        
                        remaining_excess -= convertible
                        
                        if remaining_excess <= 0:
                            break
        
        return jobs
    
    def _apply_weekly_over_40_rule(self, jobs: List[Dict]) -> List[Dict]:
        """
        CRITICAL RULE: Apply weekly over 40 hours rule
        Ensures NO employee has more than 40 regular hours per week
        """
        # Calculate total regular hours from ALL jobs (including pre-split regular hours)
        total_regular_hours = sum(job['regular_hours'] for job in jobs)
        
        # If total regular hours <= 40, we're good
        if total_regular_hours <= 40:
            return jobs
        
        # We have excess regular hours that must become overtime
        excess = total_regular_hours - 40
        
        # Sort ALL jobs by date/time (latest first) to apply rule backward through week
        # Only exclude Sunday and call-out jobs from conversion (they're already OT)
        # Include pre-split jobs in the conversion process
        eligible_jobs = [job for job in jobs
                        if job['regular_hours'] > 0
                        and 'Sunday Work' not in job['ot_reasons']
                        and 'Call Out' not in job['ot_reasons']]
        
        jobs_sorted = sorted(eligible_jobs, key=lambda x: (x['work_date'], x['start_time']), reverse=True)
        
        remaining_excess = excess
        converted_jobs = []
        
        for job in jobs_sorted:
            if remaining_excess <= 0:
                break
                
            if job['regular_hours'] > 0:
                # How much can we convert from this job?
                convertible = min(job['regular_hours'], remaining_excess)
                
                # Convert regular to OT
                job['regular_hours'] -= convertible
                job['overtime_hours'] += convertible
                job['ot_reasons'].append('Over 40 Hours/Week')
                
                remaining_excess -= convertible
                converted_jobs.append(job['job_id'])
                
                # Update calculation notes
                job['calculation_notes'] += f" | Converted {convertible:.2f}h reg->OT (weekly >40)"
        
        # Log the conversion
        if converted_jobs:
            self.calculation_log.append(
                f"Weekly >40 rule applied: Converted {excess:.2f}h regular->OT for {len(converted_jobs)} jobs"
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
        """Create summary statistics"""
        summary = []
        
        for employee in results_df['employee_name'].unique():
            emp_data = results_df[results_df['employee_name'] == employee]
            
            # Count OT reasons for this employee
            ot_reason_counts = {}
            for reasons_list in emp_data['ot_reasons']:
                for reason in reasons_list:
                    ot_reason_counts[reason] = ot_reason_counts.get(reason, 0) + 1
            
            # Calculate per-week totals instead of cumulative totals
            week_groups = emp_data.groupby('week_start')
            weekly_regular_hours = []
            weekly_overtime_hours = []
            
            for week_start, week_data in week_groups:
                week_regular = week_data['regular_hours'].sum()
                week_ot = week_data['overtime_hours'].sum()
                weekly_regular_hours.append(week_regular)
                weekly_overtime_hours.append(week_ot)
            
            # Calculate totals and maximums
            total_regular_hours = sum(weekly_regular_hours)
            total_overtime_hours = sum(weekly_overtime_hours)
            max_weekly_regular = max(weekly_regular_hours) if weekly_regular_hours else 0
            max_weekly_overtime = max(weekly_overtime_hours) if weekly_overtime_hours else 0
            
            # Check compliance: Max weekly regular should never exceed 40
            is_compliant = max_weekly_regular <= 40
            
            summary.append({
                'employee_name': employee,
                'total_regular_hours': max_weekly_regular,  # MAX weekly regular (compliance metric)
                'total_overtime_hours': total_overtime_hours,  # Cumulative OT is fine
                'total_hours': total_regular_hours + total_overtime_hours,  # Cumulative total
                'cumulative_regular_hours': total_regular_hours,  # True cumulative for reference
                'max_weekly_regular': max_weekly_regular,
                'max_weekly_overtime': max_weekly_overtime,
                'weeks_worked': len(weekly_regular_hours),
                'jobs_processed': len(emp_data),
                'call_out_jobs': len(emp_data[emp_data['is_call_out']]),
                'sunday_hours': emp_data[emp_data['day_of_week'] == 'Sunday']['overtime_hours'].sum(),
                'over_10_day_jobs': ot_reason_counts.get('Over 10 Hours/Day', 0),
                'over_40_week_jobs': ot_reason_counts.get('Over 40 Hours/Week', 0),
                'pre_split_jobs': ot_reason_counts.get('Pre-Split in Source Data', 0),
                'weeks_processed': emp_data['week_start'].nunique(),
                'compliance_status': 'COMPLIANT' if is_compliant else 'NON-COMPLIANT',
                'weekly_regular_breakdown': f"{len(weekly_regular_hours)} weeks: " + ", ".join([f"{h:.1f}h" for h in weekly_regular_hours]) if len(weekly_regular_hours) > 1 else f"{max_weekly_regular:.1f}h"
            })
        
        return summary
    
    def export_results(self, results_df: pd.DataFrame, filtered_df: pd.DataFrame, 
                      output_file: str = 'aep_overtime_results.xlsx'):
        """Export results to Excel file with multiple sheets"""
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main results sheet - format for easy reading
            export_df = results_df.copy()
            export_df['ot_reasons_text'] = export_df['ot_reasons'].apply(lambda x: ', '.join(x) if x else '')
            
            # Reorder columns for better presentation
            column_order = [
                'employee_name', 'work_date', 'day_of_week', 'week_start', 'job_id',
                'start_time', 'end_time', 'raw_duration', 'rounded_duration',
                'regular_hours', 'overtime_hours', 'ot_reasons_text',
                'is_call_out', 'lunch_deduction', 'is_pre_split', 'overlap_status',
                'calculation_notes',
                'week_regular_total', 'week_ot_total'
            ]
            
            # Only include columns that exist
            available_columns = [col for col in column_order if col in export_df.columns]
            export_df = export_df[available_columns]
            
            export_df.to_excel(writer, sheet_name='final_data', index=False)
            
            # Filtered records sheet
            if not filtered_df.empty:
                filtered_df.to_excel(writer, sheet_name='filtered_records', index=False)
            
            # Summary sheet
            summary_data = self._create_summary(results_df)
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='employee_summary', index=False)
            
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
        report.append(f"Total Jobs Processed: {total_jobs}")
        report.append(f"Total Regular Hours: {total_regular:.2f}")
        report.append(f"Total Overtime Hours: {total_ot:.2f}")
        report.append(f"Total Hours: {(total_regular + total_ot):.2f}")
        report.append("")
        
        # Check for employees with >40 regular hours PER WEEK (should be none after fix)
        emp_summary = results_df.groupby(['employee_name', 'week_start'])['regular_hours'].sum().reset_index()
        emp_summary['week_regular_hours'] = emp_summary['regular_hours']
        over_40_weeks = emp_summary[emp_summary['week_regular_hours'] > 40]
        
        if len(over_40_weeks) > 0:
            report.append("⚠️  WEEKS WITH >40 REGULAR HOURS (ERROR)")
            report.append("-" * 40)
            for _, week in over_40_weeks.iterrows():
                report.append(f"{week['employee_name']} - Week {week['week_start']}: {week['week_regular_hours']:.2f}h")
            report.append("")
        else:
            report.append("✅ VALIDATION: No weeks exceed 40 regular hours")
            report.append("")
        
        # OT breakdown
        ot_reasons = {}
        for reasons_list in results_df['ot_reasons']:
            for reason in reasons_list:
                ot_reasons[reason] = ot_reasons.get(reason, 0) + 1
        
        if ot_reasons:
            report.append("OVERTIME BREAKDOWN")
            report.append("-" * 20)
            for reason, count in sorted(ot_reasons.items()):
                ot_hours = results_df[results_df['ot_reasons'].apply(lambda x: reason in x)]['overtime_hours'].sum()
                report.append(f"{reason}: {count} jobs, {ot_hours:.2f} hours")
            report.append("")
        
        # Filter summary
        if not filtered_df.empty:
            report.append("FILTERED RECORDS")
            report.append("-" * 20)
            filter_reasons = filtered_df['filter_reason'].value_counts()
            for reason, count in filter_reasons.items():
                report.append(f"{reason}: {count} records")
            report.append("")
        
        return "\n".join(report)

# Flask Routes
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
            
            # Generate summary for display
            summary_data = calculator._create_summary(results_df)
            detailed_report = calculator.generate_detailed_report(results_df, filtered_df)
            
            # Calculate key metrics for display
            total_employees = results_df['employee_name'].nunique()
            total_jobs = len(results_df)
            total_regular = results_df['regular_hours'].sum()
            total_ot = results_df['overtime_hours'].sum()
            
            # Check compliance
            emp_summary = results_df.groupby(['employee_name', 'week_start'])['regular_hours'].sum().reset_index()
            over_40_weeks = emp_summary[emp_summary['regular_hours'] > 40]
            compliance_status = "COMPLIANT" if len(over_40_weeks) == 0 else "NON-COMPLIANT"
            
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
                                     'over_40_weeks': len(over_40_weeks)
                                 },
                                 employee_summary=summary_data[:10],  # Show top 10 employees
                                 detailed_report=detailed_report,
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
                        <li>Sunday work = All overtime</li>
                        <li>Call-out work = All overtime</li>
                        <li>Over 10 hours/day = Overtime for excess</li>
                        <li>Over 40 hours/week = Overtime for excess</li>
                        <li>Time rounding per AEP standards</li>
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
                    <p class="card-text">Issues Found</p>
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
    print("Open your web browser and go to: http://localhost:5000")
    print("Upload your timesheet file to process overtime calculations")
    
    app.run(debug=True, host='0.0.0.0', port=4444)