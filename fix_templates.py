# Clean template files to fix the ":start_line:5 -------" artifact

# Save these as separate files in your templates/ directory

# templates/base.html
base_html = """<!DOCTYPE html>
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
</html>"""

# templates/index.html
index_html = """{% extends "base.html" %}

{% block content %}
<div class="row justify-content-center">
    <div class="col-lg-8">
        <div class="card shadow">
            <div class="card-header bg-primary text-white">
                <h2 class="card-title mb-0">AEP Overtime Processor</h2>
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
                        <button type="submit" class="btn btn-success btn-lg px-5">
                            Process File
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
{% endblock %}"""

# templates/results.html  
results_html = """{% extends "base.html" %}

{% block title %}Processing Results - AEP Overtime Calculator{% endblock %}

{% block content %}
<div class="row">
    <div class="col-12">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <h2 class="text-success">Processing Complete!</h2>
            <a href="{{ url_for('download_file') }}" class="btn btn-success btn-lg">
                Download Results ({{ download_filename }})
            </a>
        </div>
    </div>
</div>

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

<div class="row mt-4 mb-5">
    <div class="col-12 text-center">
        <a href="{{ url_for('download_file') }}" class="btn btn-success btn-lg me-3">
            Download Excel Results
        </a>
        <a href="{{ url_for('index') }}" class="btn btn-outline-primary btn-lg">
            Process Another File
        </a>
    </div>
</div>
{% endblock %}"""

# Script to create the clean template files
import os

def create_clean_templates():
    """Create clean template files without any artifacts"""
    
    # Create templates directory if it doesn't exist
    templates_dir = 'templates'
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
    
    # Write clean template files
    templates = {
        'base.html': base_html,
        'index.html': index_html,
        'results.html': results_html
    }
    
    for filename, content in templates.items():
        filepath = os.path.join(templates_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Created clean template: {filepath}")

if __name__ == "__main__":
    create_clean_templates()
    print("Clean templates created successfully!")
    print("Restart your Flask app to see the changes.")