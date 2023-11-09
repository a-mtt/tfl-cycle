import streamlit as st
from streamlit_keplergl import keplergl_static
from keplergl import KeplerGl
from map_dashboard.advanced.bigquery_helper import *
import os

st.set_page_config(layout="wide")

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')
bq_client = create_bigquery_client(credentials_path=CREDENTIALS_PATH)

#@st.cache(ttl=600, show_spinner=True)
#def get_table_data(client, dataset_name, table_name):
#    return query_all_from_table(client, dataset_name, table_name)


st.title('Page 2')
st.write('This is the content of page 2.')

# Cache the query to prevent re-running it on each app interaction

table_data = query_map_one()

map_1 = KeplerGl(height=800)
map_1.add_data(data=table_data, name='data_1')
keplergl_static(map_1)

st.write("This is a kepler.gl map in streamlit")
