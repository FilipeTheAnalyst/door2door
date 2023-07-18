import requests
from google.cloud import storage
import re
import pandas as pd
import pandas_gbq
import pyarrow.parquet as pq
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

def check_files_exist(bucket_name, file_path):
    # Initialize Google Cloud Storage client
    gcs_client = storage.Client()

    # Get the GCS bucket
    bucket = gcs_client.get_bucket(bucket_name)

    # List all files in the specified file path
    blobs = bucket.list_blobs(prefix=file_path)

    # Check if any files exist in the bucket
    if not any(blobs):
        print("No JSON files found in the GCS bucket.")
        return False

    return True

def copy_s3_bucket_to_gcs(s3_bucket_name, s3_folder_path, gcs_bucket_name, gcs_folder_path, region, file_exists):
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
    gcs_object_urls = []

    if file_exists==False:
        selected_object_urls = object_urls
    else:
        for url, last_modified in zip(object_urls, last_modified_values):
            last_modified_dt = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')
            if last_modified_dt >= one_hour_ago and last_modified_dt <= now:
                selected_object_urls.append(url)

    # Copy each selected object from S3 to Google Cloud Storage
    gcs_client = storage.Client()
    for object_url in selected_object_urls:
        s3_object_url = s3_bucket_url + object_url
        gcs_object_key = gcs_folder_path + object_url

        gcs_object_urls.append(gcs_object_key)
        
        response = requests.get(s3_object_url)
        
        gcs_bucket = gcs_client.get_bucket(gcs_bucket_name)
        blob = gcs_bucket.blob(gcs_object_key)
        blob.upload_from_string(response.content)
    
    print("Copied objects list: ", gcs_object_urls)

    return gcs_object_urls 

def read_json_from_gcs(bucket_name, file_path, parquet_filename, selected_objects):
    # Initialize Google Cloud Storage client
    gcs_client = storage.Client()

    # Get the GCS bucket
    bucket = gcs_client.get_bucket(bucket_name)

    # Initialize an empty list to store the merged DataFrames
    merged_dfs = []

    # Iterate over each selected JSON file
    for object_url in selected_objects:
        blob = bucket.get_blob(object_url)
        if blob:
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

    # Check if any files were processed
    if not merged_dfs:
        print("No JSON files were processed.")
        return None

    # Concatenate all the merged DataFrames
    merged_df = pd.concat(merged_dfs, ignore_index=True)

    # Change the column names if needed
    merged_df = merged_df.rename(columns={'at': 'created_at', 'location.lat': 'location_lat', 'location.lng': 'location_lng', 'location.at': 'location_created_at'})

    merged_df.to_parquet(parquet_filename, index=False)

    return parquet_filename


def save_parquet_to_bigquery(parquet_filename, dataset_name, project_id):
    # Check if the Parquet file exists
    if parquet_filename is not None and os.path.exists(parquet_filename):
        table_name = f'{dataset_name}.raw_data'

        # Read the Parquet file into a Pandas DataFrame
        parquet_table = pq.read_table(parquet_filename)
        dataframe = parquet_table.to_pandas()

        # Save the merged DataFrame to BigQuery table
        pandas_gbq.to_gbq(
            dataframe,
            table_name,
            project_id=project_id,
            if_exists='append'
        )

        print(f"Saved merged DataFrame to BigQuery table: {table_name}")
    else:
        print(f"Parquet file does not exist or no file name provided.")


def delete_parquet_file(parquet_filename):
    # Delete the Parquet file if it exists
    if parquet_filename and os.path.exists(parquet_filename):
        os.remove(parquet_filename)
        print(f"Deleted Parquet file: {parquet_filename}")
    else:
        print(f"Parquet file does not exist or no file name provided.")


# S3 and GCS bucket and file path information
bucket_name = 'loka_data_lake_door2door'
gcs_folder_path = 'raw/'
file_path = 'raw/data/'
dataset_name = 'door2door'
project_id = 'hallowed-name-392510'
current_dir = os.getcwd()
current_date = datetime.now().strftime("%Y%m%d")
parquet_filename = f"merged_data_{current_date}.parquet"
parquet_file_path = os.path.join(current_dir, parquet_filename)
s3_region = 'eu-west-1'
s3_bucket_name = 'de-tech-assessment-2022'
s3_folder_path = 'data/'


# Define the DAG
dag = DAG(
    'copy_s3_to_gcs_and_process_json_v2',
    description='Copy S3 bucket to Google Cloud Storage and process JSON',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define the tasks
check_files_exist_task = PythonOperator(
    task_id='check_files_exist',
    python_callable=check_files_exist,
    op_kwargs={'bucket_name': bucket_name, 'file_path': file_path},
    dag=dag
)

copy_s3_to_gcs_task = PythonOperator(
    task_id='copy_s3_to_gcs',
    python_callable=copy_s3_bucket_to_gcs,
    op_kwargs={
        's3_bucket_name': s3_bucket_name,
        's3_folder_path': s3_folder_path,
        'gcs_bucket_name': bucket_name,
        'gcs_folder_path': gcs_folder_path,
        'region': s3_region,
        'file_exists': check_files_exist_task.output
    },
    provide_context=True,
    dag=dag
)

process_json_task = PythonOperator(
    task_id='process_json',
    python_callable=read_json_from_gcs,
    op_kwargs={'bucket_name': bucket_name, 'file_path': file_path, 'parquet_filename': parquet_file_path, 'selected_objects': copy_s3_to_gcs_task.output},
    dag=dag
)

save_to_bigquery_task = PythonOperator(
    task_id='save_to_bigquery',
    python_callable=save_parquet_to_bigquery,
    op_kwargs={'parquet_filename': process_json_task.output, 'dataset_name': dataset_name, 'project_id': project_id},
    provide_context=True,
    dag=dag
)

delete_parquet_file_task = PythonOperator(
    task_id='delete_parquet_file',
    python_callable=delete_parquet_file,
    op_kwargs={'parquet_filename': process_json_task.output},
    dag=dag
)

# Set task dependencies
check_files_exist_task >> copy_s3_to_gcs_task >> process_json_task >> save_to_bigquery_task >> delete_parquet_file_task