import pandas as pd
import os
import time
import logging
from google.cloud import storage

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

BUCKET_NAME = "tfl-cycle-1413"

def get_wanted_files_dates(bucket_name:str, yyyymm:str):
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    iterator = bucket.list_blobs(prefix='bronze/')

    get_fnames = [i.name.split("/")[-1] for i in iterator]
    get_dates = [i[:7] for i in get_fnames]

    file_list = []
    for f_date, f_names in zip(get_dates, get_fnames) :
        if f_date.startswith(yyyymm):
            file_list.append(f'bronze/{f_names}')

    return [
        {
        "filename": f'./data/{file}',
        "object_name": file
        }
    for file in file_list]

def prepare_data(current_date:str, **context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='get_files_names')

    for file in file_list:
        fi = file["filename"].split("/")[-1]
        logging.info(f'Opening the following file: ./data/bronze/{fi}')
        logging.info(os.path.getsize(f'./data/bronze/{fi}'))
        df = pd.read_parquet(f'./data/bronze/{fi}')
        is_old_format="Rental Id" in df.columns

        df = df.rename(columns={'Rental Id': 'Number',
                                    'Start Date': 'Start date',
                                    'End Date': 'End date',
                                    'StartStation Name': 'Start station',
                                    'Start Station Name': 'Start station',
                                    'Duration_Seconds': 'Total duration',
                                    'EndStation Name': 'End station',
                                    'End Station Name': 'End station',
                                    'Duration': 'Total duration',
                                    'Bike Id': 'Bike number'
                                    })
        if not is_old_format:
            df = df.drop(['Bike model','Total duration'], axis=1)
            df = df.rename(columns={"Total duration (ms)":"Total duration"})
            df['Total duration'] = df['Total duration'].map(lambda x: x*0.001)
        cols_to_keep=['Number', 'Start date', 'Start station',
       'End date', 'End station', 'Bike number', 'Total duration']
        df=df[cols_to_keep]
        df.dropna(inplace=True)
        df['Total duration'] = df['Total duration'].map(lambda x: int(x))
        df['Bike number'] = df['Bike number'].map(lambda x: int(x))

        df["date"] = current_date

        df.to_parquet(f'./data/silver/{fi}')

def download_from_gcs(bucket_name:str, **context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='get_files_names')

    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    for file in file_list:
        fi = file["filename"].split("/")[-1]
        blob = bucket.blob(f'bronze/{fi}')
        blob.download_to_filename(f'./data/bronze/{fi}')
        time.sleep(15)

def upload_to_gcs(bucket_name,**context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='get_files_names')

    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    for file in file_list:
        fi = file["filename"].split("/")[-1]
        blob = bucket.blob(f'silver/{fi}')
        blob.upload_from_filename(f'./data/silver/{fi}')

with DAG(
    "transform",
    start_date=datetime(2015, 1, 1, tzinfo=timezone.utc),
    schedule_interval='@monthly',
    default_args={"depends_on_past": True},
    catchup=True,
) as dag:

    current_year_month = '{{ds_nodash[:6]}}'
    current_date = '{{ds_nodash}}'
    gcp_con_id='google_conn'

    get_files_names_task = PythonOperator(
        task_id="get_files_names",
        python_callable=get_wanted_files_dates,
        op_kwargs={"bucket_name": BUCKET_NAME,
                   "yyyymm":current_year_month}
    )

    # download_from_gcs_task = PythonOperator(
    #     task_id="download_from_gcs",
    #     python_callable=download_from_gcs,
    #     op_kwargs={"bucket_name": BUCKET_NAME},
    #     provide_context=True
    # )
    download_with_gcs_op = GCSToLocalFilesystemOperator.partial(
        task_id="download_from_gcs",
        gcp_conn_id=gcp_con_id,
        bucket=BUCKET_NAME
    ).expand_kwargs(get_files_names_task.output)

    prepare_data_task = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        op_kwargs={"current_date":current_date},
        provide_context=True,
        retries=3
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={"bucket_name": BUCKET_NAME},
        provide_context=True,
        retries=3
    )

    get_files_names_task >> download_with_gcs_op >> prepare_data_task >> upload_to_gcs_task
