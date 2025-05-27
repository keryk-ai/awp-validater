from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import io
from datetime import datetime, timedelta

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

def detect_exceptions(df):
    exceptions = []
    seen = set()

    for _, row in df.iterrows():
        key = (
            row.get('Resource Name'),
            row.get('Validated Start Time'),
            row.get('Validated End Time'),
            row.get('Client Job #')
        )
        if key in seen:
            exceptions.append(row)
        else:
            seen.add(key)

    grouped = df.groupby('Resource Name')
    for name, group in grouped:
        sorted_group = group.sort_values('Validated Start Time')
        for i in range(len(sorted_group) - 1):
            current_end = sorted_group.iloc[i]['Validated End Time']
            next_start = sorted_group.iloc[i + 1]['Validated Start Time']
            if pd.notna(current_end) and pd.notna(next_start) and current_end > next_start:
                exceptions.append(sorted_group.iloc[i])
                exceptions.append(sorted_group.iloc[i + 1])

    return pd.DataFrame(exceptions).drop_duplicates()

@app.post("/process")
async def process_file(request: Request, file: UploadFile = File(...)):
    df = pd.read_excel(file.file, sheet_name="Raw Data")

    # Standardize and preprocess times
    df['Validated Start Time'] = pd.to_datetime(df['Validated Start Time'], errors='coerce')
    df['Validated End Time'] = pd.to_datetime(df['Validated End Time'], errors='coerce')

    # Handle cross-day time spans
    df['Duration'] = (df['Validated End Time'] - df['Validated Start Time']).dt.total_seconds() / 3600
    df['Duration'] = df['Duration'].apply(lambda x: round(x * 4) / 4 if pd.notna(x) else 0)

    # Handle lunch deduction if applicable
    if 'Lunch Deduction' in df.columns:
        df['Lunch Deducted'] = df['Lunch Deduction'].fillna('').apply(lambda x: 0.5 if str(x).lower() == 'yes' else 0.0)
    else:
        df['Lunch Deducted'] = 0.0

    df['Adjusted Duration'] = df['Duration'] - df['Lunch Deducted']

    # Add mock overtime calculations
    df['Straight Time'] = df['Adjusted Duration'].apply(lambda x: min(x, 10))
    df['Overtime'] = df['Adjusted Duration'].apply(lambda x: max(0, x - 10))
    df['OT Reason'] = df['Overtime'].apply(lambda x: 'Over 10' if x > 0 else '')

    # Create exceptions tab
    exceptions_df = detect_exceptions(df)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name="Processed Data", index=False)
        exceptions_df.to_excel(writer, sheet_name="Exceptions", index=False)

    output.seek(0)
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={"Content-Disposition": "attachment; filename=processed_overtime.xlsx"})