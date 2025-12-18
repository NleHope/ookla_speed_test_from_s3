import os
import boto3
from botocore.exceptions import ClientError
from helpers import load_cfg

# Define configuration file path here
CFG_FILE = "./utils/config.yaml"

def get_s3_client(datalake_cfg):
    """Initialize MinIO Client"""
    return boto3.client(
        's3',
        endpoint_url=f"http://{datalake_cfg['endpoint']}",
        aws_access_key_id=datalake_cfg['access_key'],
        aws_secret_access_key=datalake_cfg['secret_key']
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """
    Check if bucket exists.
    If not (404 error) -> Create new.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' not found. Creating new...")
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Successfully created bucket: {bucket_name}")
            except ClientError as ce:
                print(f"Error creating bucket: {ce}")
                return False
        else:
            print(f"Unknown error checking bucket: {e}")
            return False
    return True

def ingest_data(config_path):
    # 1. Load Config
    cfg = load_cfg(config_path)
    if not cfg: return

    data_cfg = cfg['data']
    lake_cfg = cfg['datalake']

    # 2. Connect to MinIO
    s3_client = get_s3_client(lake_cfg)
    bucket_name = lake_cfg['bucket_name']
    target_folder = lake_cfg['folder_name']
    source_dir = data_cfg['folder_path']

    # 3. Check & Create Bucket
    if not ensure_bucket_exists(s3_client, bucket_name):
        print("Stopping program due to Bucket error.")
        return

    print(f"--- Starting Ingest from: {source_dir} ---")
    
    # 4. Check source directory
    if not os.path.exists(source_dir):
        print(f"Error: Source directory '{source_dir}' does not exist.")
        return

    # 5. Walk files and Upload
    count = 0
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            
            # Create S3 key path
            relative_path = os.path.relpath(local_file_path, source_dir)
            s3_object_name = os.path.join(target_folder, relative_path).replace("\\", "/")

            try:
                s3_client.upload_file(local_file_path, bucket_name, s3_object_name)
                print(f"Uploaded: {file} -> {bucket_name}/{s3_object_name}")
                count += 1
            except Exception as e:
                print(f"Error uploading file {file}: {e}")

    print(f"--- Completed! Total {count} files. ---")

if __name__ == "__main__":
    # Updated to use the CFG_FILE variable
    ingest_data(CFG_FILE)