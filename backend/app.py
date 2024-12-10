from flask import Flask, jsonify, request, send_from_directory
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from marshmallow import ValidationError
from schemas import FileDownloadSchema,FileUploadSchema, handle_validation_error
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

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

@app.route('/newreport/<form>', methods=['POST'])
def new_report(new_report_form):
    pass

@app.route('/viewreport/<report_id>', methods=['GET'])
def view_report(report_id):
    pass


if __name__ == '__main__':
    # Ensure the main upload folder exists when the app starts
    ensure_folder(UPLOAD_FOLDER)
    app.run(host='0.0.0.0', port=5000, debug=True)
