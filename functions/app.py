from flask import Flask, request, render_template, send_file
from flask_cors import CORS # Import CORS
import pandas as pd
import io
import os # Import os module
from aep_overtime_calculator import AEPOvertimeCalculator

# Get the absolute path to the directory containing this script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Construct paths to templates and static folders relative to the project root
template_dir = os.path.join(current_dir, '..', 'templates')
static_dir = os.path.join(current_dir, '..', 'static')

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
CORS(app) # Enable CORS for all routes

# Set maximum content length for file uploads (e.g., 16 MB)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 # 16 Megabytes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part", 400
    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400
    if file:
        try:
            # Read the file into a pandas DataFrame
            # Determine file type and read accordingly
            file_content = file.read() # Read the entire file content
            file_stream = io.BytesIO(file_content) # Create a BytesIO object from content

            if file.filename.lower().endswith('.csv'):
                df = pd.read_csv(file_stream)
            elif file.filename.lower().endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_stream)
            else:
                return "Unsupported file type", 400

            calculator = AEPOvertimeCalculator()
            processed_df, filtered_df = calculator.process_all_data(df)

            # Create an in-memory Excel file for download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                processed_df.to_excel(writer, sheet_name='Processed Data', index=False)
                filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)
            output.seek(0)

            return send_file(output, 
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             download_name='aep_overtime_results.xlsx',
                             as_attachment=True)
        except Exception as e:
            return f"Error processing file: {e}", 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4444)