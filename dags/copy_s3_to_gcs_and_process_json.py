from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from google.cloud import storage
import re
import pandas as pd
import pandas_gbq
from google.cloud import storage

def copy_s3_bucket_to_gcs(s3_bucket_name, s3_folder_path, gcs_bucket_name, gcs_folder_path, region):
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

def read_json_from_gcs(bucket_name, file_path):
    # Initialize Google Cloud Storage client
    gcs_client = storage.Client()

    # Get the GCS bucket
    bucket = gcs_client.get_bucket(bucket_name)

    # List all files in the specified file path
    blobs = bucket.list_blobs(prefix=file_path)

    # Initialize an empty list to store the merged DataFrames
    merged_dfs = []

    # Iterate over each JSON file
    for blob in blobs:
        # Download the JSON file from GCS to a string
        json_data = blob.download_as_text()

        # Convert the JSON data to a DataFrame
        df = pd.read_json(json_data, lines=True)

        # Normalize the 'data' field based on the nested keys
        nested_columns = ['data.id', 'data.location']
        df_normalized = pd.json_normalize(df['data'])

        # Merge the normalized DataFrame with the original DataFrame
        df_merged = pd.concat([df, df_normalized], axis=1)

        # Drop the 'data' column
        df_merged = df_merged.drop(columns=['data'])

        # Append the merged DataFrame to the list
        merged_dfs.append(df_merged)

    # Concatenate all the merged DataFrames
    merged_df = pd.concat(merged_dfs, ignore_index=True)

    # Change the column names if needed
    merged_df = merged_df.rename(columns={'at': 'created_at' 'location.lat': 'location_lat', 'location.lng': 'location_lng', 'location.at': 'location_created_at'})

    return merged_df

def save_dataframe_to_bigquery(dataframe, dataset_name, project_id):
    table_name = f'{dataset_name}.raw_data'
    partition_field = 'at'  # Replace with the actual partition field name

    # Convert the "at" column to datetime if it's not already in the correct format
    if not pd.api.types.is_datetime64_any_dtype(dataframe[partition_field]):
        dataframe[partition_field] = pd.to_datetime(dataframe[partition_field])

    # Save the merged DataFrame to BigQuery table with partitioning
    pandas_gbq.to_gbq(
        dataframe,
        table_name,
        project_id=project_id,
        if_exists='replace',
        partitioning_field=partition_field
    )
    
    print(f"Saved merged DataFrame to partitioned BigQuery table: {table_name}")

# GCS bucket and file path information
bucket_name = 'loka_data_lake_door2door'
file_path = 'raw/data/'
dataset_name = 'door2door'
project_id = 'hallowed-name-392510'

# Define the DAG
dag = DAG(
    'copy_s3_to_gcs_and_process_json',
    description='Copy S3 bucket to Google Cloud Storage and process JSON',
    schedule_interval=@daily,
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define the tasks
copy_s3_to_gcs_task = PythonOperator(
    task_id='copy_s3_to_gcs',
    python_callable=copy_s3_bucket_to_gcs,
    op_kwargs={
        's3_bucket_name': 'de-tech-assessment-2022',
        's3_folder_path': 'data/',
        'gcs_bucket_name': 'loka_data_lake_door2door',
        'gcs_folder_path': 'raw/',
        'region': 'eu-west-1'
    },
    dag=dag
)

process_json_task = PythonOperator(
    task_id='process_json',
    python_callable=read_json_from_gcs,
    op_kwargs={'bucket_name': bucket_name, 'file_path': file_path},
    dag=dag
)

save_to_bigquery_task = PythonOperator(
    task_id='save_to_bigquery',
    python_callable=save_dataframe_to_bigquery,
    op_kwargs={'dataset_name': dataset_name, 'project_id': project_id},
    provide_context=True,
    dag=dag
)

# Set task dependencies
copy_s3_to_gcs_task >> process_json_task >> save_to_bigquery_task