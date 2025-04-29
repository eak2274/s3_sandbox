import boto3
import pandas as pd
from io import BytesIO
from config import ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, REGION, BUCKET_NAME
from botocore.client import Config

import os
os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "WHEN_REQUIRED"

# Создание клиента S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version='s3v4')
)

bucket_name = 'bucket-000002'
input_file = 'input/data.csv'
output_file = 'output/data_processed.csv'

try:
    # 1. Скачивание файла
    print(f"Downloading {input_file}...")
    response = s3_client.get_object(Bucket=bucket_name, Key=input_file)
    csv_content = response['Body'].read()
    
    # 2. Обработка данных
    df = pd.read_csv(BytesIO(csv_content))  # Чтение напрямую из bytes
    filtered_df = df[df['value'] < 50]
    print("Filtered data:\n", filtered_df)
    
    # 3. Подготовка к загрузке с использованием BytesIO
    with BytesIO() as buffer:
        filtered_df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)  # Перемотка в начало буфера
        
        # 4. Загрузка обратно
        print(f"Uploading to {output_file}...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_file,
            Body=buffer,
            ContentType='text/csv',
            ContentLength=buffer.getbuffer().nbytes  # Явное указание длины
        )
    
    print("File processed and uploaded successfully!")

except Exception as e:
    print(f"Error: {str(e)}")