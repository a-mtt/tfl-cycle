import pandas as pd
import os

from google.cloud import storage

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow.utils.log.logging_mixin import LoggingMixin

BUCKET_NAME = "tfl-cycle-1413"

def get_wanted_files_dates(bucket_name:str, yyyymm:str):
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    iterator = bucket.list_blobs(prefix='bronze/')
    response = iterator._get_next_page_response()

    get_file_names = [i['name'].split("/")[-1] for i in response['items']]
    get_dates = [i[:7] for i in get_file_names]

    file_list = []
    for f_date, f_names in zip(get_dates, get_file_names) :
        if f_date.startswith(yyyymm):
            file_list.append(f'./data/{f_names}')

    return file_list

#def get_wanted_files_objects():
#    return [f'/app/airflow/data/{f.split("/")[-1]}' for f in get_wanted_files_dates(BUCKET_NAME, current_year_month)]

def prepare_data(file_path:str, current_date:str):
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

def download_from_gcs(bucket:str, filename:str, objectname:str):
    print("Hello from download_from_gcs")
    print(filename, objectname)
    download_from_bucket_task = GCSToLocalFilesystemOperator(
        task_id=f"download_to_local",
        bucket=bucket,
        #filename="./data/20230612.parquet",
        #object_name="/app/airflow/data/20230612.parquet",
        filename=filename,
        object_name=objectname,
        gcp_conn_id="google_cloud_default"
    )
    return download_from_bucket_task

with DAG(
    "transform",
    start_date=datetime(2015, 1, 1, tzinfo=timezone.utc),
    schedule_interval='@weekly',
    default_args={"depends_on_past": True},
    catchup=True,
) as dag:

    current_year_month = '{{ds_nodash[:6]}}'
    current_date = '{{ds_nodash}}'

    start_task = EmptyOperator(
        task_id="start_task"
    )

    get_wanted_files = get_wanted_files_dates(BUCKET_NAME,current_year_month)
    get_wanted_objects = [f'/app/airflow/data{f.split("/")[-1]}' for f in get_wanted_files]
    print("****************************************HELLO***********")
    #files_list_task = PythonOperator(
    #    task_id="files_list",
    #    python_callable=get_wanted_files_dates,
    #    op_kwargs={"yyyymm":current_year_month,
    #               "bucket_name":BUCKET_NAME}
    #)

    tasks_array =[]

    for gwf, gwo in zip(get_wanted_files, get_wanted_objects):
        download_files_to_local_task = PythonOperator(
            task_id="download_files_to_load_{gwf}",
            python_callable=download_from_gcs,
            op_kwargs={"bucket":BUCKET_NAME,
                       "filename": gwf,
                       "objectname": gwo})

        tasks_array.append(download_files_to_local_task)

    start_task >> tasks_array

"""    download_from_bucket_task = GCSToLocalFilesystemOperator(
        task_id=f"download_to_local",
        bucket=BUCKET_NAME,
        #filename="./data/20230612.parquet",
        #object_name="/app/airflow/data/20230612.parquet",
        filename=get_wanted_files_dates()[0],
        object_name=get_wanted_files_dates()[1],
        gcp_conn_id="google_cloud_default"
    )

    transform_data_task = PythonOperator(
        task_id="add_date_column",
        python_callable=prepare_data,
        op_kwargs={"file_path": file_path,
        "current_date":current_date}
    )"""

    #upload_data_task = LocalFilesystemToGCSOperator(
    #task_id="upload_file",
    #src=UPLOAD_FILE_PATH,
    #dst=FILE_NAME,
    #bucket=BUCKET_NAME,
    #)

"""download_from_bucket_task >> transform_data_task"""
