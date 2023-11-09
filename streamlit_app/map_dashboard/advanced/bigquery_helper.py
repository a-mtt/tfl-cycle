# bigquery_helper.py
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


# Function to create BigQuery client
def create_bigquery_client(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
    )
    client = bigquery.Client(credentials=credentials, location="europe-west9")
    return client

# Function to query all data from a BigQuery table
@st.cache_data(ttl=600, show_spinner=True)
#def query_all_from_table(client, dataset_name, table_name):
#    query = f"SELECT * FROM `{dataset_name}.{table_name}`"
#    query_job = client.query(query)
#    return query_job.result().to_dataframe()

def query_all_from_table(credentials_path, dataset_name, table_name, query, location="US"):
    if len(query) < 1:
        query = f"SELECT * FROM `{dataset_name}.{table_name}` WHERE Total_duration__ms_ < 8000000 "
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
    )
    client = bigquery.Client(credentials=credentials, location=location)
    query_job = client.query(query)
    return query_job.result().to_dataframe()



# Add additional query functions as needed
