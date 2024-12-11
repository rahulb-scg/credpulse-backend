# Standard library imports
import os
from datetime import datetime

# Third-party imports
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from marshmallow import ValidationError
from werkzeug.utils import secure_filename

# Local imports
from backend.schemas import FileDownloadSchema, FileUploadSchema, NewReportSchema, handle_validation_error
from backend.db.mongo import save_report, get_report
import backend.main as main

load_dotenv()

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', './uploads')
ALLOWED_EXTENSIONS = set(os.getenv('ALLOWED_EXTENSIONS', '').split(','))
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/', methods=['GET'])
def show_home():
    return jsonify({"message": "Hello from the Python backend!"})

# Ensure the upload folder exists
def ensure_folder(foldername=UPLOAD_FOLDER):
    if not os.path.exists(foldername):
        os.makedirs(foldername)

# Function to check if the uploaded file is allowed
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload', methods=['POST'])
def upload_files():

    # Validate request using Marshmallow
    try:
        data = FileUploadSchema().load(request.files)
    except ValidationError as err:
        return handle_validation_error(err)

    if 'files' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    files = request.files.getlist('files')  # Get list of files
    if not files:
        return jsonify({'error': 'No selected files'}), 400

    # Create a timestamped subfolder for this upload session
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    upload_subfolder = os.path.join(app.config['UPLOAD_FOLDER'], timestamp)
    ensure_folder(upload_subfolder)  # Ensure the folder exists

    uploaded_files = []

    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)  # Sanitize filename
            file_path = os.path.join(upload_subfolder, filename)
            file.save(file_path)  # Save the file to the subfolder
            uploaded_files.append(filename)
        else:
            return jsonify({'error': f'File type not allowed for {file.filename}'}), 400

    return jsonify({'message': 'Files uploaded successfully', 'files': uploaded_files}), 201

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):

    # Validate request using Marshmallow
    try:
        schema = FileDownloadSchema()
        schema.load({"filename": filename})

    except ValidationError as err:
        return handle_validation_error(err)

    """Endpoint to download a specific file."""
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    # Log the file path for debugging
    print(f"Looking for file at: {file_path}")

    if os.path.exists(file_path):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)
    
    return jsonify({'error': 'File not found'}), 404

@app.route('/newreport', methods=['POST'])
def new_report():
    """Endpoint to create a new report with data and config file uploads."""
    try:
        # Validate request form data
        schema = NewReportSchema()
        form_data = request.form.to_dict()
        form_data['data_file'] = request.files.get('data_file')
        form_data['config_file'] = request.files.get('config_file')
        schema.load(form_data)
    except ValidationError as err:
        return handle_validation_error(err)
    
    # Extract form data
    report_name = form_data['report_name']
    description = form_data.get('description', '')

    # Create report folder and save files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_folder = os.path.join(app.config['UPLOAD_FOLDER'], f"{report_name}_{timestamp}")
    ensure_folder(report_folder)

    # Save files
    data_file = request.files.get('data_file')
    config_file = request.files.get('config_file')
    
    data_file_path = os.path.join(report_folder, secure_filename(data_file.filename))
    config_file_path = os.path.join(report_folder, secure_filename(config_file.filename))

    data_file.save(data_file_path)
    config_file.save(config_file_path)

    # Run analysis
    result = main.main(config_file_path, data_file_path)

    # Prepare report data for MongoDB
    report_data = {
        "id": timestamp,
        "type": "tmas",
        "columns": {
            "loanid": {
                "column_name": "loanid",
                "header": "Loan Identifier"
            }
        },
        "created_at": datetime.now().isoformat(),
        "date": {
            "start_date": "",
            "end_date": ""
        },
        "file": {
            "name": data_file.filename,
            "size": os.path.getsize(data_file_path),
            "type": data_file.content_type,
            "url": data_file_path
        },
        "processed_at": datetime.now().isoformat(),
        "processed_url": "",
        "rejected_at": "",
        "user_id": ""
    }

    # Save to MongoDB
    report_id = save_report(report_data)

    # Return response with MongoDB ID
    return jsonify({
        "message": "Report created successfully",
        "report_id": report_id
    }), 201


@app.route('/viewreport/<report_id>', methods=['GET'])
def view_report(report_id):
    """Endpoint to view a specific report by its ID."""
    try:
        report = get_report(report_id)
        
        if report:
            return jsonify({
                "message": "Report found",
                "report": report
            }), 200
        else:
            return jsonify({
                "error": "Report not found"
            }), 404
            
    except Exception as e:
        return jsonify({
            "error": f"Error retrieving report: {str(e)}"
        }), 500

if __name__ == '__main__':
    # Ensure the main upload folder exists when the app starts
    ensure_folder(UPLOAD_FOLDER)
    app.run(host='0.0.0.0', port=5000, debug=True)
