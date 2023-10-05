#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Transferr data from files in S3 using a GCP bucket as staging area before loading into a BigQuery database for explorring and analysis.
  
Credentials are managed with external files to ensure access and privacy control.

BigQuery data is set in GCP beforehand with and empty database to receive the data.

"""

from google.cloud import storage
import ast
from google.cloud import bigquery
import os
import io
import boto3 # for s3 access

# Set up GCP storage bucket
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './gcp_oauth.json'

gcs_bucket_name = 'trip_advisor_journey'
gcs_prefix = 'clickstream2'

gcs_client = storage.Client()
  
# Read s3 credential
with open('auth_creds') as f:
    creds_file = f.read()
credentials = ast.literal_eval(creds_file)

S3_URI = credentials['s3_uri']
S3_KEY_ID = credentials['s3_access_key_id']
S3_KEY_SECRET = credentials['s3_access_key_secret']

# Configure S3 access
s3_client = boto3.client(
    's3',
    aws_access_key_id=S3_KEY_ID,
    aws_secret_access_key=S3_KEY_SECRET
)

# Files transfer from S3 to GCP
s3_objects = s3_client.list_objects_v2(Bucket='ng-data-science-interviews',Prefix='clickstream2/')

for s3_object in s3_objects['Contents']:
    s3_key = s3_object['Key']
    gcs_file_name = os.path.basename(s3_key)
    
    # Stream file from S3 bucket
    data_stream = io.BytesIO()
    s3_client.download_fileobj(
        Bucket='ng-data-science-interviews',
        Key=s3_key,
        Fileobj=data_stream
    )
    
    # Upload to cloud storage
    bucket_gcs = gcs_client.get_bucket(gcs_bucket_name)
    blob = bucket_gcs.blob(s3_key)
    blob.upload_from_file(data_stream, rewind=True)
    print(f"File {s3_key} loaded into GCP bucket.") 

# Load the Parquet files from GCS into BigQuery
PROJECT_ID = 'tripadvisorjourneys'
DATASET_ID = 'journey_to_tripadvisor'
TABLE_NAME = 'consolidated_raw_journeys'

bigquery_client = bigquery.Client(project=PROJECT_ID)
dataset_ref = bigquery_client.dataset(DATASET_ID)
table_ref = dataset_ref.table(TABLE_NAME)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.PARQUET
job_config.autodetect = True

uri = f"gs://{gcs_bucket_name}/{gcs_prefix}/*.parquet"

load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)
load_job.result()  

print(f"Loaded {load_job.output_rows} rows into {table_ref.path}")



