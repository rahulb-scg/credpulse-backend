import os
from datetime import datetime
import logging
from typing import Dict, Any

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from marshmallow import ValidationError
from werkzeug.utils import secure_filename

from backend.config import config
from backend.schemas import FileDownloadSchema, FileUploadSchema, NewReportSchema, handle_validation_error
from backend.db.mongo import save_report, get_report, list_reports
from backend.main import main

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Configure app from config
app.config['UPLOAD_FOLDER'] = config.flask['upload_folder']
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_EXTENSIONS = config.flask['allowed_extensions']

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@app.route('/', methods=['GET'])
def show_home():
    return jsonify({"message": "Hello from the Python backend!"})

def ensure_folder(foldername: str = None):
    """Ensure the specified folder exists."""
    if foldername is None:
        foldername = app.config['UPLOAD_FOLDER']
    if not os.path.exists(foldername):
        os.makedirs(foldername)
        logging.info(f"Created folder: {foldername}")

def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.errorhandler(413)
def request_entity_too_large(error):
    logging.error(f"File too large error: {error}")
    return jsonify({'error': 'File too large'}), 413

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file upload endpoint."""
    try:
        data = FileUploadSchema().load(request.files)
    except ValidationError as err:
        return handle_validation_error(err)

    if 'files' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': 'No selected files'}), 400

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    upload_subfolder = os.path.join(app.config['UPLOAD_FOLDER'], timestamp)
    ensure_folder(upload_subfolder)

    uploaded_files = []
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(upload_subfolder, filename)
            file.save(file_path)
            uploaded_files.append(filename)
            logging.info(f"File '{filename}' uploaded successfully")
        else:
            logging.warning(f"File type not allowed: {file.filename}")
            return jsonify({'error': f'File type not allowed for {file.filename}'}), 400

    return jsonify({
        'message': 'Files uploaded successfully',
        'files': uploaded_files
    }), 201

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename: str):
    """Download a specific file."""
    try:
        schema = FileDownloadSchema()
        schema.load({"filename": filename})
    except ValidationError as err:
        return handle_validation_error(err)

    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    logging.debug(f"Looking for file at: {file_path}")

    if os.path.exists(file_path):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)
    return jsonify({'error': 'File not found'}), 404

@app.route('/newreport', methods=['POST'])
def new_report():
    """Create a new report with data and config file uploads."""
    try:
        schema = NewReportSchema()
        form_data = request.form.to_dict()
        form_data['config_file'] = request.files.get('config_file')
        schema.load(form_data)
        logging.debug(f"Form data validated: {form_data}")
    except ValidationError as err:
        logging.error(f"Validation error: {err.messages}")
        return handle_validation_error(err)

    report_name = form_data['report_name']
    description = form_data.get('description', '')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_folder = os.path.join(app.config['UPLOAD_FOLDER'], f"{report_name}_{timestamp}")
    ensure_folder(report_folder)

    # Save config file
    config_file = request.files.get('config_file')
    config_file_path = os.path.join(report_folder, secure_filename(config_file.filename))
    config_file.save(config_file_path)
    logging.info(f"Config file saved: {config_file_path}")

    # Handle data file if provided
    data_file_path = None
    if 'data_file' in request.files and request.files['data_file']:
        data_file = request.files['data_file']
        data_file_path = os.path.join(report_folder, secure_filename(data_file.filename))
        data_file.save(data_file_path)
        logging.info(f"Data file saved: {data_file_path}")

    try:
        # Process data using main module
        result = main(config_file_path, data_file_path)

        # Prepare report data
        report_data = {
            "report_name": report_name,
            "description": description,
            "type": "tmas",
            "created_at": datetime.utcnow().isoformat(),
            "status": "completed",
            "files": {
                "config_name": config_file.filename,
                "config_size": os.path.getsize(config_file_path),
                "config_type": config_file.content_type,
                "config_path": config_file_path,
                "data_name": data_file.filename if data_file_path else None,
                "data_size": os.path.getsize(data_file_path) if data_file_path else 0,
                "data_type": data_file.content_type if data_file_path else None,
                "data_path": data_file_path
            },
            "result": result
        }

        # Save to MongoDB
        report_id = save_report(report_data)
        logging.info(f"Report saved with ID: {report_id}")

        return jsonify({
            "message": "Report created successfully",
            "report_id": report_id
        }), 201

    except Exception as e:
        logging.error(f"Error processing report: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/viewreport/<report_id>', methods=['GET'])
def view_report(report_id: str):
    """View a specific report by its ID."""
    try:
        report = get_report(report_id)
        if report:
            return jsonify({
                "message": "Report found",
                "report": report
            }), 200
        return jsonify({"error": "Report not found"}), 404
    except Exception as e:
        logging.error(f"Error retrieving report: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/listreports', methods=['GET'])
def list_reports_route():
    """List reports with pagination."""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            return jsonify({"error": "Page number must be greater than 0"}), 400
        if page_size < 1 or page_size > 100:
            return jsonify({"error": "Page size must be between 1 and 100"}), 400

        result = list_reports(page=page, page_size=page_size)
        return jsonify({
            "message": "Reports retrieved successfully",
            "data": result
        }), 200

    except Exception as e:
        logging.error(f"Error listing reports: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
