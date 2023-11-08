import os
from datetime import datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)

BUCKET_NAME = "tfl-cycle-1413"
PROJECT_ID = ""
DATASET_ID = ""
TABLE_ID=""

with DAG(
    "load",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2023, 12, 12),
    schedule_interval='@weekly',
    default_args={"depends_on_past": False},
    catchup=False
) as dag:
    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        #gcp_conn_id="google_cloud_default",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        #gcp_conn_id="google_cloud_default",
        dataset_id=DATASET_ID,
        project_id=PROJECT_ID,
        table_id=TABLE_ID,
        schema_fields=[
            {"name": "Number", "type": "INT64", "mode": "REQUIRED"},
            {"name": "Start date", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "Start station number", "type": "INT64", "mode": "REQUIRED"},
            {"name": "Start station", "type": "STRING", "mode": "REQUIRED"},
            {"name": "End date", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "End station number", "type": "INT64", "mode": "REQUIRED"},
            {"name": "End station", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Bike number", "type": "INT64", "mode": "REQUIRED"},
            {"name": "Total duration", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date", "type": "STRING", "mode": "REQUIRED"},
        ],
    )

    remove_existing_data_task = BigQueryInsertJobOperator(
        task_id="remove_existing_data",
        project_id=PROJECT_ID,
        #gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"DELETE FROM wagon1314-de.bike_uk.usage WHERE date = '2022-01-01'",
                "useLegacySql": False,
            }
        },
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=f"335JourneyDataExtract12Sep2022-18Sep2022.parquet",
        #gcp_conn_id="google_cloud_default",
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )

create_dataset_task >> create_table_task >> remove_existing_data_task >> load_to_bigquery_task
