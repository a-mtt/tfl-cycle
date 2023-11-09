import os
from datetime import datetime
import logging
from airflow import DAG
from google.cloud import storage

from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)

# #### testing jinja #####
# from jinja2 import Environment, select_autoescape

# def quote(s):
#     return f"'{s}'"

# env = Environment(autoescape=select_autoescape(['html', 'xml']))
# env.filters['quote']= quote
# #########################



def get_wanted_files_dates(bucket_name:str, yyyymm:str):
    logging.info(f"Using the following date: {yyyymm}")
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))
    bucket = storage_client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix='silver/')

    get_file_names = [blob.name.split("/")[-1] for blob in blob_list]
    get_dates = [i[:7] for i in get_file_names]

    file_list = []
    for f_date, f_names in zip(get_dates, get_file_names) :
        if f_date.startswith(yyyymm):
            file_list.append(f'silver/{f_names}')
    logging.info(f"Returned file list is the following: {file_list}")
    return file_list



BUCKET_NAME = os.environ.get("BUCKET_NAME")
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
DATASET_ID = "tfl_project"
TABLE_ID = os.environ.get("TABLE_ID")
TABLE_ID = "updated_silver"

with DAG(
    "tfl_load",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2023,11,1),
    schedule_interval='@monthly',
    default_args={"depends_on_past": True,
                  'retry_exponential_backoff': True},
    catchup=True
) as dag:
    date='{{ds_nodash[:6]}}'
    gcp_con_id='google_conn'
    # schema=[
    #         {"name": "Number", "type": "INT64", "mode": "Nullable"},
    #         {"name": "Total duration", "type": "INT64", "mode": "Nullable"},
    #         {"name": "Start station number", "type": "INT64", "mode": "Nullable"},
    #         {"name": "Start date", "type": "STRING", "mode": "Nullable"},
    #         {"name": "Start station", "type": "STRING", "mode": "Nullable"},
    #         {"name": "End date", "type": "STRING", "mode": "Nullable"},
    #         {"name": "End station number", "type": "INT64", "mode": "Nullable"},
    #         {"name": "End station", "type": "STRING", "mode": "Nullable"},
    #         {"name": "Bike number", "type": "INT64", "mode": "Nullable"},
    #         {"name": "date", "type": "STRING", "mode": "Nullable"},
    #     ]


    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id=gcp_con_id,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )

    # create_table_task = BigQueryCreateEmptyTableOperator(
    #     task_id="create_table",
    #     gcp_conn_id=gcp_con_id,
    #     dataset_id=DATASET_ID,
    #     project_id=PROJECT_ID,
    #     table_id=TABLE_ID,
    #     schema_fields=schema
    # )

    get_wanted_files_dates_task = PythonOperator(
        task_id="get_wanted_files_dates",
        python_callable=get_wanted_files_dates,
        op_kwargs={
            "bucket_name":BUCKET_NAME,
            "yyyymm":date
        }
    )

    remove_existing_data_task = BigQueryInsertJobOperator(
        task_id="remove_existing_data",
        project_id=PROJECT_ID,
        gcp_conn_id=gcp_con_id,
        configuration={
            "query": {
                "query": f"""DELETE FROM {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} WHERE date IN ({{{{ "\'" +task_instance.xcom_pull(task_ids='get_wanted_files_dates')|map('replace', '.parquet', '')|map('replace', 'silver/', '')|join("\', \'") + "\'"  }}}})""",
                "useLegacySql": False,
            }
        },
    )


    load_to_bigquery_task = GCSToBigQueryOperator.partial(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_format='PARQUET',
        gcp_conn_id=gcp_con_id,
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        skip_leading_rows=None,
        write_disposition="WRITE_APPEND",
        autodetect=True
        #schema_fields=schema
    ).expand(source_objects=get_wanted_files_dates_task.output)

create_dataset_task >> get_wanted_files_dates_task >> remove_existing_data_task >> load_to_bigquery_task
