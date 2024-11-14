import os
import boto3
from botocore.exceptions import NoCredentialsError

s3_config = {
    'bucket_name': 'credpulse-test', 
    'folder_prefix': '', 
    'local_dir': './test', 
    'aws_access_key_id': 'AKIAYPLVC5CQLX3SD37V', 
    'aws_secret_access_key': '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', 
    'aws_region': 'us-east-1'
}

def download_test_data(
        bucket_name=s3_config['bucket_name'], 
        folder_prefix=s3_config['folder_prefix'], 
        local_dir=s3_config['local_dir'], 
        aws_access_key_id=s3_config['aws_access_key_id'], 
        aws_secret_access_key=s3_config['aws_secret_access_key'], 
        aws_region=s3_config['aws_region']):
    """
    Connects to an S3 bucket, fetches all files, and saves them to a local directory.
    
    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_prefix (str): The folder path (prefix) within the bucket to download files from.
        local_dir (str): Local directory to save the downloaded files.
        aws_access_key_id (str, optional): AWS access key ID. If None, it uses default credentials.
        aws_secret_access_key (str, optional): AWS secret access key. If None, it uses default credentials.
        aws_region (str, optional): AWS region for the S3 bucket. Default is 'us-east-1'.
    
    Returns:
        None
    """
    
    # Create the local directory if it doesn't exist
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    try:
        # Connect to S3
        s3 = boto3.client(
            's3',
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        # List all objects in the bucket
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        
        if 'Contents' not in objects:
            print(f"No files found in folder: {folder_prefix} within bucket: {bucket_name}")
            return
        
        # Download each file to the local directory
        for obj in objects['Contents']:
            file_key = obj['Key']

            # Skip if it's the folder itself (some folders are listed as keys with a trailing '/')
            if file_key.endswith('/'):
                continue

            # Build the local file path
            relative_path = os.path.relpath(file_key, folder_prefix)  # Get relative path within the folder
            local_file_path = os.path.join(local_dir, relative_path)

            # Ensure the directory structure exists
            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))
            
            print(f"Downloading {file_key} to {local_file_path}...")
            s3.download_file(bucket_name, file_key, local_file_path)
            print(f"{file_key} downloaded successfully!")
    
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except Exception as e:
        print(f"Error downloading files from S3: {e}")

def upload_test_data(
        bucket_name=s3_config['bucket_name'], 
        folder_prefix=s3_config['folder_prefix'], 
        local_dir=s3_config['local_dir'], 
        aws_access_key_id=s3_config['aws_access_key_id'], 
        aws_secret_access_key=s3_config['aws_secret_access_key'], 
        aws_region=s3_config['aws_region']):
    """
    
    """

    # Connect to S3
    s3 = boto3.client(
        's3',
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            
            # Construct the S3 file path
            if folder_prefix:
                # Add the S3 folder as a prefix to the S3 key (file path)
                s3_file_path = os.path.join(folder_prefix, os.path.relpath(local_file_path, local_dir)).replace("\\", "/")
            else:
                # Just the relative path from the local folder
                s3_file_path = os.path.relpath(local_file_path, local_dir).replace("\\", "/")

            try:
                # Upload the file
                print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_file_path}...")
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
            except FileNotFoundError:
                print(f"File {local_file_path} not found.")
            except NoCredentialsError:
                print("Credentials not available.")

    print("Folder upload completed.")

    
if __name__ == '__main__':
    download_test_data()