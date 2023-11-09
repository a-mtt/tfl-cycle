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

def query_main_page():
    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = """
                SELECT
                    *
                FROM `data-eng-q4-23.tfl_project.rides_facts`
                WHERE LEFT(date,4)="2015"
                LIMIT 10000

    """
    query_job = client.query(query)
    return query_job.result().to_dataframe()

def query_map_one():

    client = create_bigquery_client(credentials_path = CREDENTIALS_PATH)
    query = """
                    WITH stations AS (
                SELECT
                    *
                FROM `data-eng-q4-23.tfl_project.stations_dim`),
                rides AS (
                SELECT
                    *
                FROM `data-eng-q4-23.tfl_project.rides_facts`
                WHERE LEFT(date,4)="2015"),
                district AS (
                SELECT
                    *
                FROM `data-eng-q4-23.tfl_project.district_dim`
                ),
                geo AS (
                SELECT
                    station_number,
                    d.district,
                    district_lon,
                    district_lat
                FROM stations s
                LEFT JOIN district d ON s.district = d.district
                ),

                join_table AS (
                SELECT
                    r.*,
                    g_s.district AS district_start,
                    g_e.district AS district_end,
                    g_s.district_lon AS lon_start,
                    g_s.district_lat AS lat_start,
                    g_e.district_lon AS lon_end,
                    g_e.district_lat AS lat_end
                FROM rides r
                LEFT JOIN geo g_s ON r.Start_station = g_s.station_number
                LEFT JOIN geo g_e ON r.End_station = g_e.station_number ),

                agg_table AS (
                SELECT
                    lon_start,
                    lat_start,
                    lat_end,
                    lon_end,
                    district_start,
                    district_end,
                    count(*) AS nb_rides
                FROM join_table
                GROUP BY 1,2,3,4,5,6),

                rank_table AS (
                SELECT
                    *,
                    RANK() OVER (PARTITION BY district_start ORDER BY nb_rides DESC) as rank
                FROM agg_table )

                SELECT
                *
                FROM rank_table
                WHERE rank = 1 """

    query_job = client.query(query)
    return query_job.result().to_dataframe()
