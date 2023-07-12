import requests
from google.cloud import storage
import re

def copy_s3_bucket_to_gcs(s3_bucket_name, s3_folder_path, gcs_bucket_name, gcs_folder_path):
    # Construct the S3 bucket URL
    s3_bucket_url = f"https://{s3_bucket_name}.s3.{region}.amazonaws.com/"

    # Retrieve the list of objects in the S3 folder
    response = requests.get(s3_bucket_url)

    # Extract the object URLs from the response content
    object_urls = re.findall(r'<Key>(.*?)<\/Key>', response.text)

    # Filter object URLs for JSON files within the specified folder
    filtered_object_urls = [
        url for url in object_urls
        if url.startswith(s3_folder_path) and url.endswith('.json')
    ]

    # Copy each object from S3 to Google Cloud Storage
    gcs_client = storage.Client()
    for object_url in object_urls:
        s3_object_url = s3_bucket_url + object_url
        gcs_object_key = gcs_folder_path + object_url

        response = requests.get(s3_object_url)

        gcs_bucket = gcs_client.get_bucket(gcs_bucket_name)
        blob = gcs_bucket.blob(gcs_object_key)
        blob.upload_from_string(response.content)

# Usage
s3_bucket_name = 'de-tech-assessment-2022'
s3_folder_path = 'data/'
gcs_bucket_name = 'loka_data_lake_door2door'
gcs_folder_path = 'raw/'
region = 'eu-west-1'  # Replace with the actual region of the S3 bucket

copy_s3_bucket_to_gcs(s3_bucket_name, s3_folder_path, gcs_bucket_name, gcs_folder_path)