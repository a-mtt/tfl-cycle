import pandas as pd
import os

from google.cloud import storage

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

current_date = "{{ ds[:7] }}"
BUCKET_NAME = "tfl-cycle-1413"

def get_all_files_dates(bucket_name:str):
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    iterator = bucket.list_blobs(prefix='bronze/')
    response = iterator._get_next_page_response()

    return [i['name'][7:15] for i in response['items']]

def prepare_data(current_date: str):

    for fn_date in get_all_files_dates(BUCKET_NAME):

        df = pd.read_parquet("./data/{fn_date}.parquet")

        while fn_date < "20220912" :
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
        df["date"] = current_date

        df.to_parquet("./data/silver/{fn_date}.parquet")

with DAG(
    "transform",
    start_date=datetime.now(),
    end_date=datetime(2023, 12, 12),
    schedule_interval='*/5 * * * *',
    default_args={"depends_on_past": False},
    catchup=True
) as dag:

    download_from_bucket_task = GCSToLocalFilesystemOperator(
        task_id="download_to_local",
        bucket=BUCKET_NAME,
        filename="./data/20230612.parquet",
        object_name="/app/airflow/data/20230612.parquet",
        gcp_conn_id="google_cloud_default"
    )

    transform_data_task = PythonOperator(
        task_id="add_date_column",
        python_callable=prepare_data,
        op_kwargs={"current_date":current_date}
    )

download_from_bucket_task >> transform_data_task
