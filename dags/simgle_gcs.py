import pandas as pd
import os

from google.cloud import storage

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET_NAME = "tfl-cycle-1413"

def get_wanted_files_dates(bucket_name:str, yyyymm:str):
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    iterator = bucket.list_blobs(prefix='bronze/')
    response = iterator._get_next_page_response()

    get_fnames = [i['name'].split("/")[-1] for i in response['items']]
    get_dates = [i[:7] for i in get_fnames]

    file_list = []
    for f_date, f_names in zip(get_dates, get_fnames) :
        if f_date.startswith(yyyymm):
            file_list.append(f'./data/{f_names}')

    return file_list

def prepare_data(current_date:str, **context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='get_files_names')

    for file_path in file_list:
        df = pd.read_parquet(file_path)

        if "Rental Id" in df.columns:
            df = df.rename(columns={'Rental Id': 'Number',
                                        'Start Date': 'Start date',
                                        'End Date': 'End date',
                                        'StartStation Id':'Start station number',
                                        'StartStation Name': 'Start station',
                                        'EndStation Id': 'End station number',
                                        'EndStation Name': 'End station',
                                        'Duration': 'Total duration',
                                        'Bike Id': 'Bike number'
                                        })
        else:
            df = df.drop(['Bike model','Total duration'], axis=1)
            df = df.rename(columns={"Total duration (ms)":"Total duration"})
            df['Total duration'] = df['Total duration'].map(lambda x: x*0.001)
            df = df.round({'Total duration':0})

        df["date"] = current_date

        df.to_parquet(f'./data/silver/{file_path.split("/")[-1]}')

def download_from_gcs(bucket_name:str, **context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='get_files_names')
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    path=f'./data'
    for file in file_list:
        blob = bucket.blob(f'bronze/{file}')
        blob.download_to_filename(os.path.join(path, file))

with DAG(
    "simple_GCS",
    start_date=datetime(2015, 1, 1, tzinfo=timezone.utc),
    schedule_interval='@weekly',
    default_args={"depends_on_past": True},
    catchup=True,
) as dag:

    current_year_month = '{{ds_nodash[:6]}}'
    current_date = '{{ds_nodash}}'

    get_files_names_task = PythonOperator(
        task_id="get_files_names",
        python_callable=get_wanted_files_dates,
        op_kwargs={"bucket_name": BUCKET_NAME,
                   "yyyymm":current_year_month}
    )

    prepare_data_task = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        op_kwargs={"current_date":current_date},
        provide_context=True
    )

    download_from_gcs_task = PythonOperator(
        task_id="download_from_gcs",
        python_callable=download_from_gcs,
        op_kwargs={"bucket_name": BUCKET_NAME},
        provide_context=True
    )

    get_files_names_task >> prepare_data_task >> download_from_gcs_task
