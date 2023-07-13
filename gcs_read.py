import pandas as pd
import pandas_gbq
from google.cloud import storage
from datetime import datetime, timedelta, timezone

def read_json_from_gcs(bucket_name, file_path):
    # Initialize Google Cloud Storage client
    gcs_client = storage.Client()

    # Get the GCS bucket
    bucket = gcs_client.get_bucket(bucket_name)

    # List all files in the specified file path
    blobs = bucket.list_blobs(prefix=file_path)

   # Get the datetime for one hour ago in the local timezone
    one_hour_ago = datetime.now() - timedelta(hours=1)
    one_hour_ago = one_hour_ago.replace(tzinfo=timezone.utc).astimezone()

    # Initialize an empty list to store the merged DataFrames
    merged_dfs = []

    # Iterate over each JSON file
    for blob in blobs:
        # Get the last modified timestamp of the file
        last_modified = blob.updated

        # Check if the file was last modified during the last hour
        if last_modified >= one_hour_ago:
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

    if merged_dfs:
        # Concatenate all the merged DataFrames
        merged_df = pd.concat(merged_dfs, ignore_index=True)

        # Change the column names if needed
        merged_df = merged_df.rename(columns={'at': 'created_at', 'location.lat': 'location_lat', 'location.lng': 'location_lng', 'location.at': 'location_created_at'})

        return merged_df
    else:
        print("No JSON files found within the last hour.")
        return None

def save_dataframe_to_bigquery(dataframe, dataset_name, project_id):
    table_name = f'{dataset_name}.raw_data'
    pandas_gbq.to_gbq(dataframe, table_name, project_id='hallowed-name-392510', if_exists='replace')
    print(f"Saved merged DataFrame to BigQuery table: {table_name}")

if __name__ == '__main__':
    # GCS bucket and file path information
    bucket_name = 'loka_data_lake_door2door'
    file_path = 'raw/data/'
    dataset_name = 'door2door'
    project_id = 'hallowed-name-392510'
    # Read JSON data from GCS for each file in the specified path and save the merged result to BigQuery
    merged_df = read_json_from_gcs(bucket_name, file_path)

    print(merged_df)
    # save_dataframe_to_bigquery(merged_df, dataset_name, project_id)

