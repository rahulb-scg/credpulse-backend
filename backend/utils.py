import os
import json
import pandas as pd
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

from backend.config import config
from backend.db.mongo import MongoDBClient

def get_absolute_filepath(filename: str) -> str:
    """Get absolute filepath for a given filename."""
    upload_folder = config.flask['upload_folder']
    return os.path.join(upload_folder, filename)

def file_type_handler(file_path: str) -> pd.DataFrame:
    """Handle different file types and return a pandas DataFrame."""
    file_extension = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_extension == '.csv':
            return pd.read_csv(file_path)
        elif file_extension == '.json':
            return pd.read_json(file_path)
        elif file_extension in ['.xls', '.xlsx']:
            return pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise

def upload_file_to_s3(file_name: str, bucket: str, object_name: Optional[str] = None) -> bool:
    """Upload a file to an S3 bucket."""
    if object_name is None:
        object_name = file_name

    aws_config = config.aws
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_config['access_key_id'],
        aws_secret_access_key=aws_config['secret_access_key'],
        region_name=aws_config['region']
    )

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        logging.info(f"Successfully uploaded {file_name} to {bucket}/{object_name}")
        return True
    except ClientError as e:
        logging.error(f"Error uploading file to S3: {e}")
        return False

def export_output(data: pd.DataFrame, output_path: str) -> str:
    """Export DataFrame to CSV file."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        data.to_csv(output_path, index=False)
        logging.info(f"Successfully exported data to {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"Error exporting data: {e}")
        raise

def save_to_mongo(report_data: Dict[str, Any]) -> str:
    """Save report data to MongoDB."""
    mongo_client = MongoDBClient()
    try:
        report_id = mongo_client.insert_report(report_data)
        return report_id
    finally:
        mongo_client.close()

def get_test_report_config() -> Dict[str, Any]:
    """Get test report configuration."""
    return {
        'name': os.getenv('TEST_REPORT_NAME', 'TEST_REPORT'),
        'description': os.getenv('TEST_REPORT_DESCRIPTION', 'Test Report Description'),
        'model': os.getenv('TEST_REPORT_MODEL', 'TEST_MODEL'),
        'config_file_csv': os.getenv('TEST_CONFIG_FILE_CSV', 'test/test_data/test_data.json'),
        'config_file_db': os.getenv('TEST_CONFIG_FILE_DB', 'test/test_data/test_db.json')
    }

