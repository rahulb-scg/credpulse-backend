import os
import logging
from typing import Dict, Any, Optional
import pandas as pd
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

from backend.config import config
from backend.utils import (
    get_absolute_filepath,
    file_type_handler,
    export_output,
    save_to_mongo,
    upload_file_to_s3
)

# Initialize Flask app
app = Flask(__name__)

# Configure app from config
app.config['UPLOAD_FOLDER'] = config.flask['upload_folder']
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_EXTENSIONS = config.flask['allowed_extensions']

def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload endpoint."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'File type not allowed'}), 400
    
    try:
        filename = secure_filename(file.filename)
        filepath = get_absolute_filepath(filename)
        file.save(filepath)
        
        # Process the file
        df = file_type_handler(filepath)
        
        # Export processed data
        output_path = os.path.join(config.output['dir_model_ready'], config.output['file_base'])
        export_output(df, output_path)
        
        # Upload to S3 if configured
        if config.aws['access_key_id'] and config.aws['secret_access_key']:
            upload_file_to_s3(output_path, config.aws['s3_bucket_name'])
        
        # Save report to MongoDB
        report_data = {
            'filename': filename,
            'status': 'processed',
            'rows_processed': len(df)
        }
        report_id = save_to_mongo(report_data)
        
        return jsonify({
            'message': 'File processed successfully',
            'report_id': report_id
        }), 200
        
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

