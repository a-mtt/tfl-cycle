import streamlit as st
st.set_page_config(layout="wide")
from map_dashboard.advanced.bigquery_helper import query_all_from_table
import os
import numpy as np
import plotly.express as px
import pandas as pd
from streamlit_keplergl import keplergl_static
from keplergl import KeplerGl
from map_dashboard.advanced.transfo_functions import *

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')

### QUERYING DATA

### FACT DATA

query = f"SELECT * FROM `{DATASET_NAME}.{TABLE_NAME}` WHERE Total_duration < 8000000 LIMIT 10000"
table_data = query_all_from_table(CREDENTIALS_PATH, dataset_name=DATASET_NAME, table_name=TABLE_NAME, location="US", query=query)

### GEOGRAPHY DATA

import requests

url = "https://api.tfl.gov.uk/BikePoint/"
req = requests.get(url)
df_geo = pd.DataFrame(req.json())
df_geo["station_id"] = df_geo.apply(lambda x: x["additionalProperties"][0]["value"], axis=1)
df_geo =  extract_neighborhood(df_geo, column_name = "commonName", neighborhood_column_name="nbh")

nbh_df = df_geo.groupby(by=["nbh"],as_index=False).agg(
        nbh_lat=('lat', 'mean'),
        nbh_lon=('lon', 'mean')
    )

### DATA PREP - JOINING

df_final = table_data.copy()
df_final = clean_column_names(df_final)
df_final =  extract_neighborhood(df_final, column_name = "start_station", neighborhood_column_name="start_nbh")
df_final = extract_neighborhood(df_final, column_name = "end_station", neighborhood_column_name="end_nbh")

df_final = df_final.merge(nbh_df, how="left", left_on ="start_nbh", right_on="nbh")
df_final.rename(mapper={"nbh_lat":"start_lat", "nbh_lon":"start_lon"},axis=1, inplace=1)

df_final = df_final.merge(nbh_df, how="left", left_on ="end_nbh", right_on="nbh")
df_final.rename(mapper={"nbh_lat":"end_lat", "nbh_lon":"end_lon"},axis=1, inplace=1)

df_final.drop(columns=["nbh_x", "nbh_y"],axis=1,inplace=True)
df_final.head()

### DATA PREP - AGGREGATING

df_agg = df_final.groupby(by=["start_nbh", "end_nbh", "start_lat","start_lon", "end_lat", "end_lon"],as_index=False).agg(
        count=('bike_number', 'count')
    )


top_five_df = df_agg.groupby("start_nbh").apply(
      lambda x: x.nlargest(5, "count")
  ).reset_index(drop=True)

### DISPLAY

st.title('Page 1')
st.write('This is the content of page 1.')

st.dataframe(table_data)
st.dataframe(nbh_df)
st.dataframe(df_final)
st.dataframe(top_five_df)

map_1 = KeplerGl(height=400)
map_1.add_data(data=top_five_df, name='data_1')
keplergl_static(map_1)

st.write("This is a kepler.gl map in streamlit")
