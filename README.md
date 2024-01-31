# Door2Door Modern Data Stack

## Introduction
door2door collects the live position of all vehicles in its fleet in real-time via a GPS sensor in each
vehicle. These vehicles run in operating periods that are managed by door2door’s operators. An API is
responsible for collecting information from the vehicles and place it on an S3 bucket, in raw format, to
be consumed.

The purpose of this challenge is to automate the build of a simple yet scalable data lake and data
warehouse that will enable our BI team to answer questions like:

*What is the average distance traveled by our vehicles during an operating period?*

## Data Architecture

![Data_Architecture](https://github.com/FilipeTheAnalyst/loka-homework/assets/61323876/beeade85-2cb0-4cc3-a28a-f7f73663b2b0)

I identify below all the technologies used to build the data architecture:

- **AWS S3 Bucket:** Data Source
- **Google Cloud Storage (GCS):** Data Lake
- **BigQuery:** Data Warehouse
- **Terraform:** Infrastructure as a Code (IaC) used to create the GCS bucket and BigQuery dataset.
- **Google Cloud Composer (Airflow):** Workflow orchestration platform to manage the extract & load phases.
- **data build tool (dbt):** Transformation layer with the definition of data models build using the star-schema approach.

## Workflow Orchestration
Here we have the DAG defined to manage the extract & load process.

![dag](https://github.com/FilipeTheAnalyst/loka-homework/assets/61323876/c052290d-ed8a-43a0-b3e7-170e4e0f5772)

Below I give a brief explanation of the objective and purpose of each task.

- **check_files_exist:** checks if there are files already ingested into GCS bucket, in order to understand if it is the first time loading the data.
- **copy_s3_to_gcs:** copies the content of all the JSON files from the AWS S3 bucket. If the task **check_files_exist** returns True, it will only copy files modified during the last hour.
- **process_json:** reads all the JSON files ingested into GCS bucket that were processed by **copy_s3_to_gcs** task and normalize the data so we can store it on a relational database.
- **save_to_bigquery:** stores the data into a table on BigQuery dataset
- **delete_parquet_file:** deletes the parquet file created by **process_json** task.

  [**Code Used**](dags/copy_s3_to_gcs_and_process_json.py)

## Data Model

![data_model](https://github.com/FilipeTheAnalyst/loka-homework/assets/61323876/11a9eb91-5cfd-4cda-a135-d64dbdf3e2ad)

## Transformation Layer (dbt)

[**Data Models Defined**](https://github.com/FilipeTheAnalyst/loka-homework/tree/master/dbt_door2door/models/)

