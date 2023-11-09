# bigquery_helper.py
import streamlit as st
import os
from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')

# Function to query data

def create_bigquery_client(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
    )
    client = bigquery.Client(credentials=credentials, location="US")
    return client

@st.cache_data
def query_main_aggregated_rides():
    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = "SELECT * FROM `data-eng-q4-23.tfl_project.rides_daily_total`"
    query_job = client.query(query)
    return query_job.result().to_dataframe()

@st.cache_data
def query_main_rides_distribution():
    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = "SELECT * FROM `data-eng-q4-23.tfl_project.rides_daily_distribution`"
    query_job = client.query(query)
    return query_job.result().to_dataframe()

@st.cache_data
def query_map_one():

    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = "SELECT * FROM `data-eng-q4-23.tfl_project.map_top_routes`"
    query_job = client.query(query)
    return query_job.result().to_dataframe()

@st.cache_data
def query_start_district():
    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = "SELECT * FROM `data-eng-q4-23.tfl_project.rides_daily_per_district_start`"
    query_job = client.query(query)
    return query_job.result().to_dataframe()

@st.cache_data
def query_end_district():
    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = "SELECT * FROM `data-eng-q4-23.tfl_project.rides_daily_per_district_end`"
    query_job = client.query(query)
    return query_job.result().to_dataframe()
