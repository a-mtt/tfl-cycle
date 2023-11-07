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

def get_list_files(storage_url: str) -> list:
    """
    Reads an S3 bucket url link (for TfL cycling data)
    Returns a list of usage-stats csv files
    """
    page=requests.get(storage_url)
    soup=BeautifulSoup(page.text,features='xml')
    pattern=re.compile(r'usage-stats.*csv')
    keys=soup.find_all('Key',string=pattern)
    return [key.text for key in keys]

def modify_filename(filename: str) -> str:
    """
    Reads a filename like 01aJourneyDataExtract10Jan16-23Jan16.csv
    Returns a filename like 20160110.parquet
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
    return f'{year}{month}{day}'

def csv_to_parquet(storage_url: str, storage_path: str) -> None:
    """
    Reads a csv file with pandas and saves it as parquet
    """
    download_link=storage_url+storage_path
    df=pd.read_csv(download_link.replace(" ","%20"),low_memory=False)
    df.to_parquet(f'{AIRFLOW_HOME}/data/{modify_filename(download_link)}.parquet')


def get_monthly_files(storage_url:str, yyyymm:str)-> list:
    """
    Reads the list of all files in usage-stats and saves them locally to parquet
    """
    all_files=get_list_files(storage_url)
    date_named_files=[modify_filename(file) for file in all_files]
    file_list=[]
    for date_name,file in zip(date_named_files,all_files):
        if date_name.startswith(yyyymm):
            csv_to_parquet(storage_url,file)
            file_list.append(f'{date_name}.parquet')
    return file_list

def upload_monthly_bucket(bucket_name,**context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='download_csv')
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    path=f'{AIRFLOW_HOME}/data'
    for file in file_list:
        blob = bucket.blob(f'bronze/{file}')
        blob.upload_from_filename(os.path.join(path,file))



with DAG(
    "tfl_elt_monthly",
    schedule_interval="@monthly",
    start_date=datetime(2015,1,1,tzinfo=timezone.utc),
    catchup=True,
    description="Getting monthly data from tfl",
    default_args={"depends_on_past": True}
) as dag:
    s3_url='https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/'
    date='{{ds_nodash[:6]}}'
    download_task=PythonOperator(
        task_id="download_csv",
        python_callable=get_monthly_files,
        op_kwargs={
            "storage_url":s3_url,
            "yyyymm":date
        }
    )
    upload_local_file_to_gcs_task = PythonOperator(
        task_id="upload_parquet_to_gcs",
        python_callable=upload_monthly_bucket,
        op_kwargs={
            "bucket_name":"tfl-cycle-1413"
        },
        provide_context=True
    )

    download_task >> upload_local_file_to_gcs_task
