import boto3
import pandas as pd
from io import BytesIO
from config import ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, REGION, BUCKET_NAME
from botocore.client import Config
from typing import List, Optional

import os
# 1. Uncomment when working with very large files (>1GB)
# os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "ALWAYS"
# 2. Uncomment when troubleshooting data consistency issues
# os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "ALWAYS"
# 3. Uncomment when troubleshooting network connectivity issues
# os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "NONE"

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version='s3v4')
)

def get_s3_file_url_new(bucket_name: str, file_key: str, public: bool = True, expires_in: int = 3600) -> str:
    """
    Генерирует URL для файла в OCI Object Storage.
    
    Args:
        bucket_name: Имя бакета
        file_key: Путь к файлу в бакете
        public: True - публичная ссылка, False - presigned URL
        expires_in: Время жизни presigned URL в секундах
    
    Returns:
        Прямая ссылка на файл
    """
    
    if public:
        # Извлекаем namespace и region из ENDPOINT_URL
        # Формат: https://NAMESPACE.compat.objectstorage.REGION.oraclecloud.com
        parts = ENDPOINT_URL.replace('https://', '').replace('http://', '').split('.')
        namespace = parts[0]  # axl55ulgxbqk
        region = parts[3]     # il-jerusalem-1
        
        # Новый формат URL (с 2024 года рекомендуется Oracle)
        # https://NAMESPACE.objectstorage.REGION.oci.customer-oci.com/n/NAMESPACE/b/BUCKET/o/OBJECT
        url = f"https://{namespace}.objectstorage.{region}.oci.customer-oci.com/n/{namespace}/b/{bucket_name}/o/{file_key}"
        
        print(f"Публичная ссылка: {url}")
        return url
    else:
        # Для приватного бакета - presigned URL
        try:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': file_key},
                ExpiresIn=expires_in
            )
            print(f"Presigned URL ({expires_in} сек): {presigned_url}")
            return presigned_url
        except Exception as e:
            print(f"Ошибка создания presigned URL: {e}")
            raise

def get_s3_file_url(bucket_name: str, file_key: str, public: bool = True, expires_in: int = 3600) -> str:
    """
    Генерирует URL для файла в OCI Object Storage.
    
    Args:
        bucket_name: Имя бакета
        file_key: Путь к файлу в бакете
        public: True - публичная ссылка, False - presigned URL
        expires_in: Время жизни presigned URL в секундах
    
    Returns:
        Прямая ссылка на файл
    """
    
    if public:
        # Извлекаем namespace и region из ENDPOINT_URL
        # Формат: https://NAMESPACE.compat.objectstorage.REGION.oraclecloud.com
        parts = ENDPOINT_URL.replace('https://', '').replace('http://', '').split('.')
        namespace = parts[0]  # axl55ulgxbqk
        region = parts[3]     # il-jerusalem-1
        
        # Формируем публичный URL
        # Формат OCI: https://objectstorage.REGION.oraclecloud.com/n/NAMESPACE/b/BUCKET/o/OBJECT
        url = f"https://objectstorage.{region}.oraclecloud.com/n/{namespace}/b/{bucket_name}/o/{file_key}"
        print(f"Публичная ссылка: {url}")
        return url
    else:
        # Для приватного бакета - presigned URL
        try:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': file_key},
                ExpiresIn=expires_in
            )
            print(f"Presigned URL ({expires_in} сек): {presigned_url}")
            return presigned_url
        except Exception as e:
            print(f"Ошибка создания presigned URL: {e}")
            raise

def transform(bucket_name: str, input_file: str, output_file: str):
    """
    Transforms CSV file from S3: filters rows and saves the result back.
    
    Args:
        bucket_name: Bucket name
        input_file: Path to source file in S3
        output_file: Path to save processed file in S3
    """
    try:
        # 1. Download file
        print(f"Downloading {input_file}...")
        response = s3_client.get_object(Bucket=bucket_name, Key=input_file)
        csv_content = response['Body'].read()
        
        # 2. Process data
        df = pd.read_csv(BytesIO(csv_content))
        filtered_df = df[df['value'] < 50]
        print("Filtered data:\n", filtered_df)
        
        # 3. Prepare for upload using BytesIO
        with BytesIO() as buffer:
            filtered_df.to_csv(buffer, index=False, encoding='utf-8')
            buffer.seek(0)
            
            # 4. Upload back
            print(f"Uploading to {output_file}...")
            s3_client.put_object(
                Bucket=bucket_name,
                Key=output_file,
                Body=buffer,
                ContentType='text/csv',
                ContentLength=buffer.getbuffer().nbytes
            )
        
        print("File processed and uploaded successfully!")
        
    except Exception as e:
        print(f"Error in transform: {str(e)}")


def download_file(s3_path: str, local_path: str = ''):
    """
    Downloads file from S3 to local file system.
    
    Args:
        s3_path: Path in format 'bucket_name/path/to/file'
        local_path: Local path to save the file. If empty or None, saves to current directory with original filename
    """
    try:
        parts = s3_path.split('/', 1)
        bucket_name = parts[0]
        file_key = parts[1] if len(parts) > 1 else ''
        
        # If local_path is empty, use the filename from S3 path
        if not local_path:
            local_path = os.path.basename(file_key) if file_key else 'downloaded_file'
        
        # Create directory if it doesn't exist
        local_dir = os.path.dirname(local_path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)
            print(f"Created directory: {local_dir}")
        
        print(f"Downloading {s3_path} to {local_path}...")
        s3_client.download_file(bucket_name, file_key, local_path)
        print(f"File downloaded successfully to {local_path}")
        
    except Exception as e:
        print(f"Error downloading file: {str(e)}")


def upload_file(s3_path: str, local_path: str):
    """
    Uploads local file to S3.
    
    Args:
        s3_path: Destination path in format 'bucket_name/path/to/file'
        local_path: Path to local file
    """
    try:
        parts = s3_path.split('/', 1)
        bucket_name = parts[0]
        file_key = parts[1] if len(parts) > 1 else ''
        
        print(f"Uploading {local_path} to {s3_path}...")
        s3_client.upload_file(local_path, bucket_name, file_key)
        print(f"File uploaded successfully to {s3_path}")
        
    except Exception as e:
        print(f"Error uploading file: {str(e)}")


def list_baskets() -> List[str]:
    """
    Lists all buckets in storage.
    
    Returns:
        List of bucket names
    """
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        
        print(f"Found {len(buckets)} bucket(s):")
        for bucket in buckets:
            print(f"  - {bucket}")
        
        return buckets
        
    except Exception as e:
        print(f"Error listing buckets: {str(e)}")
        return []


def list_folders_in_a_basket(basket_name: str) -> List[str]:
    """
    Lists all root folders in the specified bucket.
    
    Args:
        basket_name: Bucket name
        
    Returns:
        List of root folders
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=basket_name,
            Delimiter='/'
        )
        
        folders = []
        if 'CommonPrefixes' in response:
            folders = [prefix['Prefix'].rstrip('/') for prefix in response['CommonPrefixes']]
        
        print(f"Root folders in '{basket_name}':")
        for folder in folders:
            print(f"  - {folder}")
        
        return folders
        
    except Exception as e:
        print(f"Error listing folders: {str(e)}")
        return []


def list_files_in_a_basket(basket_name: str) -> List[str]:
    """
    Lists all files in the specified bucket.
    
    Args:
        basket_name: Bucket name
        
    Returns:
        List of file paths
    """
    try:
        files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=basket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append(obj['Key'])
        
        print(f"Files in '{basket_name}' ({len(files)} total):")
        for file in files:
            print(f"  - {file}")
        
        return files
        
    except Exception as e:
        print(f"Error listing files: {str(e)}")
        return []


def clear_basket(basket_name: str):
    """
    Deletes all files in the specified bucket.
    
    Args:
        basket_name: Bucket name
    """
    try:
        print(f"Clearing basket '{basket_name}'...")
        
        # Get list of all objects
        paginator = s3_client.get_paginator('list_objects_v2')
        deleted_count = 0
        
        for page in paginator.paginate(Bucket=basket_name):
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                
                if objects_to_delete:
                    response = s3_client.delete_objects(
                        Bucket=basket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    deleted_count += len(response.get('Deleted', []))
        
        print(f"Deleted {deleted_count} file(s) from '{basket_name}'")
        
    except Exception as e:
        print(f"Error clearing basket: {str(e)}")


def delete_file(basket_name: str, file_path: str):
    """
    Deletes a specific file from the bucket.
    
    Args:
        basket_name: Bucket name
        file_path: File path in the bucket
    """
    try:
        print(f"Deleting '{file_path}' from '{basket_name}'...")
        s3_client.delete_object(Bucket=basket_name, Key=file_path)
        print(f"File '{file_path}' deleted successfully")
        
    except Exception as e:
        print(f"Error deleting file: {str(e)}")


def main():
    """
    Function for testing individual functions.
    """
    # You can call functions here for testing
    # pass
    # download_file(s3_path='bucket-000002/Textfile.txt', local_path='downloads/Textfile.txt')

    print(get_s3_file_url_new(bucket_name='words', file_key='Textfile.txt', expires_in=3600))


if __name__ == "__main__":
    main()