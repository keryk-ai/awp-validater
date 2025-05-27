#!/usr/bin/env python3
"""
AEP Overtime Calculator
Processes employee timesheet data and applies AEP overtime rules
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

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
                          'lunch_deduction', 'job_id']
        
        for col in required_columns:
            if col not in df_renamed.columns:
                df_renamed[col] = '' if col == 'employee_name' else 0
        
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
        
        # Convert lunch deduction to float
        df['lunch_deduction'] = pd.to_numeric(df['lunch_deduction'], errors='coerce').fillna(0.0)
        
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
        
        # Add derived columns
        df['work_date'] = df['start_datetime'].dt.date
        df['day_of_week'] = df['start_datetime'].dt.day_name()
        df['week_start'] = df['start_datetime'].dt.to_period('W-MON').dt.start_time.dt.date

        return df
    
    def calculate_duration(self, start_time: datetime, end_time: datetime, 
                          lunch_deduction: float = 0.0) -> float:
        """Calculate job duration with lunch deduction"""
        if pd.isna(start_time) or pd.isna(end_time):
            return 0.0
        
        # Calculate raw duration in hours
        duration = (end_time - start_time).total_seconds() / 3600.0
        
        # Apply lunch deduction
        if lunch_deduction > 0:
            duration -= lunch_deduction
        
        # Ensure non-negative
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
        if overlaps:
            for overlap in overlaps:
                self.calculation_log.append(f"OVERLAP DETECTED for {employee_name}: {overlap}")
        
        # Initialize weekly totals
        total_regular_hours = 0.0
        total_ot_hours = 0.0
        
        # Process each job
        for idx, job in week_data.iterrows():
            # Calculate raw duration
            raw_duration = self.calculate_duration(
                job['start_datetime'], 
                job['end_datetime'], 
                job['lunch_deduction']
            )
            
            # Apply rounding
            rounded_duration = self.apply_rounding_rules(raw_duration)
            
            # Initialize job result
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
                'calculation_notes': f"Raw: {raw_duration:.2f}h, Rounded: {rounded_duration:.2f}h"
            }
            
            # Apply overtime rules in order
            
            # Rule 1: Sunday work - all overtime
            if job['day_of_week'] == 'Sunday':
                job_result['overtime_hours'] = rounded_duration
                job_result['ot_reasons'].append('Sunday Work')
                total_ot_hours += rounded_duration
                
            # Rule 2: Call out - all overtime
            elif job['is_call_out']:
                job_result['overtime_hours'] = rounded_duration
                job_result['ot_reasons'].append('Call Out')
                total_ot_hours += rounded_duration
                
            # Rule 3: Regular time (will be adjusted later for daily >10 and weekly >40)
            else:
                job_result['regular_hours'] = rounded_duration
                total_regular_hours += rounded_duration
            
            results.append(job_result)
        
        # Rule 4: Apply daily >10 hour rule
        results = self._apply_daily_over_10_rule(results)
        
        # Recalculate totals after daily rule
        total_regular_hours = sum(job['regular_hours'] for job in results)
        total_ot_hours = sum(job['overtime_hours'] for job in results)
        
        # Rule 5: Apply weekly >40 hour rule (only regular hours count)
        if total_regular_hours > 40:
            results = self._apply_weekly_over_40_rule(results, total_regular_hours)
        
        # Add week summary to each job
        for job in results:
            job['week_regular_total'] = sum(j['regular_hours'] for j in results)
            job['week_ot_total'] = sum(j['overtime_hours'] for j in results)
        
        return results
    
    def _apply_daily_over_10_rule(self, jobs: List[Dict]) -> List[Dict]:
        """Apply over 10 hours in a day rule"""
        # Group by date
        daily_groups = {}
        for job in jobs:
            date = job['work_date']
            if date not in daily_groups:
                daily_groups[date] = []
            daily_groups[date].append(job)
        
        for date, day_jobs in daily_groups.items():
            # Calculate total non-OT hours for the day
            total_regular_day = sum(job['regular_hours'] for job in day_jobs)
            
            if total_regular_day > 10:
                excess = total_regular_day - 10
                
                # Apply excess to last job(s) of the day, working backward
                day_jobs_sorted = sorted(day_jobs, key=lambda x: x['start_time'], reverse=True)
                
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
    
    def _apply_weekly_over_40_rule(self, jobs: List[Dict], total_regular: float) -> List[Dict]:
        """Apply weekly over 40 hours rule"""
        excess = total_regular - 40
        
        # Sort jobs by date (latest first) to apply rule backward through week
        jobs_sorted = sorted(jobs, key=lambda x: (x['work_date'], x['start_time']), reverse=True)
        
        remaining_excess = excess
        for job in jobs_sorted:
            if job['regular_hours'] > 0 and remaining_excess > 0:
                # Skip if this job is already all OT or call out
                if 'Sunday Work' in job['ot_reasons'] or 'Call Out' in job['ot_reasons']:
                    continue
                
                # How much can we convert from this job?
                convertible = min(job['regular_hours'], remaining_excess)
                
                # Convert regular to OT
                job['regular_hours'] -= convertible
                job['overtime_hours'] += convertible
                job['ot_reasons'].append('Over 40 Hours/Week')
                
                remaining_excess -= convertible
                
                if remaining_excess <= 0:
                    break
        
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
                'is_call_out', 'lunch_deduction', 'calculation_notes',
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
        
        # Also save a simple summary to text file
        summary_file = output_file.replace('.xlsx', '_summary.txt')
        with open(summary_file, 'w') as f:
            f.write(self.generate_detailed_report(results_df, filtered_df))
        print(f"Summary report saved to {summary_file}")
    
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
            
            summary.append({
                'employee_name': employee,
                'total_regular_hours': emp_data['regular_hours'].sum(),
                'total_overtime_hours': emp_data['overtime_hours'].sum(),
                'total_hours': emp_data['regular_hours'].sum() + emp_data['overtime_hours'].sum(),
                'jobs_processed': len(emp_data),
                'call_out_jobs': len(emp_data[emp_data['is_call_out']]),
                'sunday_hours': emp_data[emp_data['day_of_week'] == 'Sunday']['overtime_hours'].sum(),
                'over_10_day_jobs': ot_reason_counts.get('Over 10 Hours/Day', 0),
                'over_40_week_jobs': ot_reason_counts.get('Over 40 Hours/Week', 0),
                'weeks_processed': emp_data['week_start'].nunique() if 'week_start' in emp_data.columns else emp_data['work_date'].nunique()
            })
        
        return summary
    
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
        
        # Top employees by hours
        emp_summary = results_df.groupby('employee_name').agg({
            'regular_hours': 'sum',
            'overtime_hours': 'sum'
        }).reset_index()
        emp_summary['total_hours'] = emp_summary['regular_hours'] + emp_summary['overtime_hours']
        emp_summary = emp_summary.sort_values('total_hours', ascending=False)
        
        report.append("TOP 10 EMPLOYEES BY TOTAL HOURS")
        report.append("-" * 35)
        for _, emp in emp_summary.head(10).iterrows():
            report.append(f"{emp['employee_name']}: {emp['total_hours']:.2f}h "
                         f"({emp['regular_hours']:.2f} reg, {emp['overtime_hours']:.2f} OT)")
        
        return "\n".join(report)

def main():
    """Main function to run the calculator"""
    import sys
    
    calculator = AEPOvertimeCalculator()
    
    # Get file path from command line argument or use default
    file_path = sys.argv[1] if len(sys.argv) > 1 else "report1746533344036.xls"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "aep_overtime_results.xlsx"
    
    try:
        # Parse input file
        print(f"Reading file: {file_path}")
        df = calculator.parse_input_file(file_path)
        
        # Process all data
        print("Processing overtime rules...")
        results_df, filtered_df = calculator.process_all_data(df)
        
        # Export results
        print(f"Exporting results to: {output_file}")
        calculator.export_results(results_df, filtered_df, output_file)
        
        # Display summary
        print(f"\n{'='*50}")
        print(f"PROCESSING COMPLETE!")
        print(f"{'='*50}")
        print(f"Total records processed: {len(results_df)}")
        print(f"Total records filtered: {len(filtered_df)}")
        print(f"Total regular hours: {results_df['regular_hours'].sum():.2f}")
        print(f"Total overtime hours: {results_df['overtime_hours'].sum():.2f}")
        print(f"Total hours: {(results_df['regular_hours'].sum() + results_df['overtime_hours'].sum()):.2f}")
        
        # Show OT breakdown
        ot_reasons = {}
        for reasons_list in results_df['ot_reasons']:
            for reason in reasons_list:
                ot_reasons[reason] = ot_reasons.get(reason, 0) + 1
        
        if ot_reasons:
            print(f"\nOvertime breakdown:")
            for reason, count in ot_reasons.items():
                print(f"  {reason}: {count} jobs")
        
        print(f"\nResults saved to: {output_file}")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        raise

# Test function for development
def test_with_sample_data():
    """Test the calculator with sample data"""
    calculator = AEPOvertimeCalculator()
    
    # Sample data similar to what we tested
    test_data = [
        {
            'employee_name': 'AKERS, Kristen N',
            'job_id': 'JOB-3925001',
            'start_datetime': pd.Timestamp('2025-04-28 07:30:00'),
            'end_datetime': pd.Timestamp('2025-04-28 16:25:00'),
            'work_date': pd.Timestamp('2025-04-28').date(),
            'day_of_week': 'Monday',
            'week_start': pd.Timestamp('2025-04-28').date(),
            'is_call_out': False,
            'lunch_deduction': 0.0
        },
        {
            'employee_name': 'AKERS, Kristen N',
            'job_id': 'JOB-3927733',
            'start_datetime': pd.Timestamp('2025-04-29 07:30:00'),
            'end_datetime': pd.Timestamp('2025-04-29 16:01:00'),
            'work_date': pd.Timestamp('2025-04-29').date(),
            'day_of_week': 'Tuesday',
            'week_start': pd.Timestamp('2025-04-28').date(),
            'is_call_out': False,
            'lunch_deduction': 0.0
        },
        {
            'employee_name': 'AKERS, Kristen N',
            'job_id': 'JOB-3929343',
            'start_datetime': pd.Timestamp('2025-04-30 07:30:00'),
            'end_datetime': pd.Timestamp('2025-04-30 20:30:00'),
            'work_date': pd.Timestamp('2025-04-30').date(),
            'day_of_week': 'Wednesday',
            'week_start': pd.Timestamp('2025-04-28').date(),
            'is_call_out': False,
            'lunch_deduction': 0.0
        },
        {
            'employee_name': 'AKERS, Kristen N',
            'job_id': 'JOB-3931055',
            'start_datetime': pd.Timestamp('2025-05-01 07:30:00'),
            'end_datetime': pd.Timestamp('2025-05-01 13:01:00'),
            'work_date': pd.Timestamp('2025-05-01').date(),
            'day_of_week': 'Thursday',
            'week_start': pd.Timestamp('2025-04-28').date(),
            'is_call_out': False,
            'lunch_deduction': 0.0
        }
    ]
    
    df = pd.DataFrame(test_data)
    
    print("Testing with sample data...")
    results_df, filtered_df = calculator.process_all_data(df)
    
    print(f"\nTest Results:")
    for _, row in results_df.iterrows():
        print(f"{row['work_date']} ({row['day_of_week']}): Regular {row['regular_hours']:.2f}h, OT {row['overtime_hours']:.2f}h")
        if row['ot_reasons']:
            print(f"  OT Reasons: {', '.join(row['ot_reasons'])}")
    
    totals = results_df[['regular_hours', 'overtime_hours']].sum()
    print(f"\nTotals: Regular {totals['regular_hours']:.2f}h, OT {totals['overtime_hours']:.2f}h")
    
    return results_df, filtered_df

if __name__ == "__main__":
    # Uncomment the line below to run tests instead of main processing
    # test_with_sample_data()
    main()