import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.google.cloud.transfers.local_to_gcs import (LocalFilesystemToGCSOperator)

from datetime import datetime, timezone
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import storage

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

def get_list_files(storage_url: str):
    """
    Reads an S3 bucket url link (for TfL cycling data)
    Returns a list of usage-stats csv files
    """
    page=requests.get(s3_url)
    soup=BeautifulSoup(page.text,features='xml')
    pattern=re.compile(r'usage-stats.*csv')
    keys=soup.find_all('Key',string=pattern)
    return [key.text for key in keys]

def modify_filename(filename: str) -> str:
    """
    Reads a filename like 01aJourneyDataExtract10Jan16-23Jan16.csv
    Returns a filename like 20160110.pqt
    """
    # Regular expression pattern to match the date
    date_pattern = r'((\d{1,2})([A-Za-z]{2,4})(\d{2,4}))'
    # Extract dates from the strings
    date,day,month,year = re.search(date_pattern, filename).groups()
    year = year if len(year)==4 else f'20{year}'
    month=month.lower()
    month_number=["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
    if month in month_number:
        month=str(month_number.index(month)+1)
    else:
        for i,m in enumerate(month_number):
            if m.startswith(month):
                month=str(i+1)
                break
            elif month.startswith(m):
                month=str(i+1)
                break
    if len(month)!=2:
        month='0'+month
    return f'{year}{month}{day}.parquet'

def csv_to_parquet(storage_url: str, storage_path: str) -> None:
    download_link=storage_url+storage_path
    try:
        df=pd.read_csv(download_link)
        df.to_parquet(f'{AIRFLOW_HOME}/data/{modify_filename(download_link)}')
    except:
        pass

def get_all_files(storage_url:str)-> None:
    all_files=get_list_files(storage_url)
    [csv_to_parquet(storage_url,file_path) for file_path in all_files]

def upload_to_gcs(filename:str) -> None:
    #getlistfiles
    #csv_to_parquet
    #upload_to_gcs
    pass

def upload_all_to_bucket(bucket_name):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    path=f'{AIRFLOW_HOME}/data'
    for file in os.listdir(path):
        blob = bucket.blob(os.path.join(path,file))
        blob.upload_from_filename(os.path.join(path,file))

def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)



with DAG(
    "tfl_elt",
    schedule_interval="@monthly",
    start_date=datetime(2023,10,18,tzinfo=timezone.utc),
    catchup=True,
    description="Getting all data from tfl",
    default_args={"depends_on_past": True}
) as dag:
    s3_url='https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/'
    download_task=PythonOperator(
        task_id="download_csv",
        python_callable=get_all_files,
        op_kwargs={
            "storage_url":s3_url
        }
    )
    upload_local_file_to_gcs_task = PythonOperator(
        task_id="upload_parquet_to_gcs",
        python_callable=upload_all_to_bucket,
        op_kwargs={
            "bucket_name":"tfl-cycle-1413"
        }
    )

    download_task >> upload_local_file_to_gcs_task
