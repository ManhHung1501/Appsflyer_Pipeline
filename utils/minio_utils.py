import boto3
from botocore.exceptions import ClientError
from config.minio import s3_endpoint, s3_access_key, s3_secret_key, s3_cdp_bucket

def connect_minio():
    s3_client = boto3.client("s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key)
    return s3_client

def check_and_create_bucket(s3_client,bucket_name):
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} exists.")
    except ClientError as e:
        # If the bucket does not exist, create it
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket {bucket_name} does not exist. Creating bucket...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created successfully.")
        else:
            # Handle other exceptions (e.g., permissions issues)
            print(f"Error checking/creating bucket: {e}")
            raise

def read_text_file_minio(object_key):
    try:
        minio_client = connect_minio()
        object_key  = 'ad'
        # Get the object from MinIO
        response = minio_client.get_object(Bucket=s3_cdp_bucket, Key=object_key)
        
        # Read the content of the file
        file_content = response['Body'].read().decode('utf-8') 
        return file_content
    except Exception as e:
        print(f"Read file from {object_key}Failed. Error occurred: {e}")