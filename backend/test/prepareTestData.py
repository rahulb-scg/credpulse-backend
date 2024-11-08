import os
import boto3
from botocore.exceptions import NoCredentialsError

def download_test_data(
        bucket_name='credpulse-reports-bucket', 
        folder_prefix='test', 
        local_dir='./test/test_data', 
        aws_access_key_id='AKIAYPLVC5CQLX3SD37V', 
        aws_secret_access_key='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', 
        aws_region='us-east-1'):
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

if __name__ == '__main__':
    download_test_data()