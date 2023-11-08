# bigquery_helper.py

from google.cloud import bigquery
from google.oauth2 import service_account


# Function to create BigQuery client
def create_bigquery_client(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
    )
    client = bigquery.Client(credentials=credentials)
    return client

# Function to query all data from a BigQuery table
def query_all_from_table(client, dataset_name, table_name):
    query = f"SELECT * FROM `{dataset_name}.{table_name}`"
    query_job = client.query(query)
    return query_job.result().to_dataframe()

# Add additional query functions as needed
