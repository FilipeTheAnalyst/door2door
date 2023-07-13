import requests
from google.cloud import storage
import re
from datetime import datetime, timedelta

def copy_s3_bucket_to_gcs(s3_bucket_name, s3_folder_path, gcs_bucket_name, gcs_folder_path, region):
    # Construct the S3 bucket URL
    s3_bucket_url = f"https://{s3_bucket_name}.s3.{region}.amazonaws.com/"

    # Retrieve the list of objects in the S3 folder
    response = requests.get(s3_bucket_url)

    # Extract the object URLs and LastModified timestamps from the response content
    object_urls = re.findall(r'<Key>(.*?)<\/Key>', response.text)
    object_urls = [name for name in object_urls if name.endswith('.json')]
    last_modified_values = re.findall(r'<LastModified>(.*?)<\/LastModified>', response.text)

    # Filter the object URLs based on the LastModified timestamp within the last hour
    selected_object_urls = []
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)

    for url, last_modified in zip(object_urls, last_modified_values):
        last_modified_dt = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')
        if last_modified_dt >= one_hour_ago and last_modified_dt <= now:
            selected_object_urls.append(url)


    # Copy each selected object from S3 to Google Cloud Storage
    for object_url in selected_object_urls:
        s3_object_url = s3_bucket_url + object_url
        gcs_object_key = gcs_folder_path + object_url
        
        response = requests.get(s3_object_url)
        
        gcs_bucket = gcs_client.get_bucket(gcs_bucket_name)
        blob = gcs_bucket.blob(gcs_object_key)
        blob.upload_from_string(response.content)

# GCS bucket and file path information
s3_bucket_name = 'de-tech-assessment-2022'
s3_folder_path = 'data/'
gcs_bucket_name = 'loka_data_lake_door2door'
gcs_folder_path = 'raw/'
region = 'eu-west-1'

copy_s3_bucket_to_gcs(s3_bucket_name, s3_folder_path, gcs_bucket_name, gcs_folder_path, region)