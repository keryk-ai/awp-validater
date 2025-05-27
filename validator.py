import pandas as pd
import os
from glob import glob

# Set paths
final_sheets = sorted(glob("data/*Final*.xlsx"))
processed_sheets = sorted(glob("data/processed_overtime*.xlsx"))

# Helper to clean and group totals
def clean_totals(path, sheet="Final Data"):
    df = pd.read_excel(path, sheet_name=sheet)
    df = df[df['Resource Name'].notna()]
    df = df[~df['Resource Name'].str.contains("Total", na=False)]
    return df.groupby('Resource Name')['Grand Total'].sum().reset_index()

# Run comparison
results = []
for f_file, p_file in zip(final_sheets, processed_sheets):
    final_df = clean_totals(f_file)
    processed_df = clean_totals(p_file, sheet="Processed Data")
    merged = pd.merge(final_df, processed_df, on="Resource Name", how="outer", suffixes=("_Final", "_Processed"))
    merged["Difference"] = merged["Grand Total_Final"] - merged["Grand Total_Processed"]
    merged["Source"] = os.path.basename(f_file)
    results.append(merged)

final_report = pd.concat(results)
discrepancies = final_report[final_report["Difference"].abs() > 0.01]
discrepancies.to_excel("validation_discrepancies.xlsx", index=False)