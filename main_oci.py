import oci
import pandas as pd
from io import BytesIO

# Loading config
config = oci.config.from_file()

# Creating Object Storage client
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# Obtaining namespace
namespace = object_storage_client.get_namespace().data

# Printing out list of buckets
compartment_id = config["tenancy"]
buckets = object_storage_client.list_buckets(namespace, compartment_id).data
print("Buckets:")
for bucket in buckets:
    print(bucket.name)

# Bucket and files' params
bucket_name = "bucket-000002"
input_file = "input/data.csv"
output_file = "output/data_processed.csv"

try:
    # 1. Downloading a file
    print(f"Скачиваю файл {input_file}...")
    get_obj = object_storage_client.get_object(
        namespace_name=namespace,
        bucket_name=bucket_name,
        object_name=input_file
    )
    csv_content = get_obj.data.content

    # 2. Processing data with pandas
    df = pd.read_csv(BytesIO(csv_content))
    filtered_df = df[df['value'] < 50]
    print("Filtered data:")
    print(filtered_df)

    # 3. Preparing data for uploading
    output_buffer = BytesIO()
    filtered_df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)

    # 4. Uploading the processed file
    print(f"Uploading the processed file {output_file}...")
    object_storage_client.put_object(
        namespace_name=namespace,
        bucket_name=bucket_name,
        object_name=output_file,
        put_object_body=output_buffer.getvalue(),
        content_type="text/csv"
    )
    print("The file has been successfully processed and uploaded!")

except Exception as e:
    print(f"Error occurred: {str(e)}")