
import os
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage

from datetime import datetime, timezone


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

def download_with_pd(url:str,date:datetime):
    req = requests.get(url)
    df_geo = pd.DataFrame(req.json())
    df_geo["station_number"] = df_geo.apply(lambda x: x["additionalProperties"][0]["value"], axis=1)
    df_geo["old_id"]=df_geo["id"].str.replace("BikePoints_","")
    df_geo=df_geo[["old_id","station_number","commonName","lon","lat"]]
    df_geo["district"]=df_geo['commonName'].apply(lambda x: x.split(",")[1].strip())
    file_path=f'{AIRFLOW_HOME}/data/stations/station_update_{date}.parquet'
    df_geo.to_parquet(file_path)
    return file_path

def upload_to_bucket(bucket_name,**context):
    ti=context['ti']
    file_path=ti.xcom_pull(task_ids='download_with_pd')
    storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_JSON_PATH'))

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(file_path.split("data/")[1])
    blob.upload_from_filename(file_path)


with DAG(
    "tfl_stations",
    schedule_interval="@monthly",
    start_date=datetime(2015,1,1,tzinfo=timezone.utc),
    catchup=False,
    description="Getting all station data from tfl",
    default_args={"depends_on_past": False}
) as dag:
    url = "https://api.tfl.gov.uk/BikePoint/"
    date = datetime.today()
    download_task=PythonOperator(
        task_id="download_with_pd",
        python_callable=download_with_pd,
        op_kwargs={
            "url":url,
            "date":date
        }
    )
    upload_local_task = PythonOperator(
        task_id="upload_parquet_to_gcs",
        python_callable=upload_to_bucket,
        op_kwargs={
            "bucket_name":"tfl-cycle-1413"
        },
        provide_context=True
    )

    download_task >> upload_local_task
