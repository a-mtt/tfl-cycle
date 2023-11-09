import streamlit as st
import os
import numpy as np
import plotly.express as px
import pandas as pd

from map_dashboard.advanced.bigquery_helper import *
from map_dashboard.advanced.transfo_functions import *

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')

st.set_page_config(layout="wide", page_title='Home page', page_icon=':chart_with_upwards_trend:')


client = create_bigquery_client(credentials_path=CREDENTIALS_PATH)

df = query_main_aggregated_rides()
df_hist = query_main_rides_distribution()

df_district_start = query_start_district()
df_district_end = query_end_district()


# Convert 'Start_date' to datetime

df['Start_date'] = pd.to_datetime(df['Start_date'], format='mixed')

# Get the minimum and maximum dates from the DataFrame

min_date = df['Start_date'].min()
max_date = df['Start_date'].max()


class Bikesdashboard:
    def __init__(self) -> None:
        pass

    def introduction_page(self):
        """Layout the views of the dashboard"""

        #st.title("London Bikes Dashboard")

        # Main panel

        # Custom HTML and CSS to center the title
        st.markdown("""
            <style>
            .title {
                text-align: center;
            }
            </style>
            <h1 class="title">London Bikes Dashboard</h1>
        """, unsafe_allow_html=True)

        # Add a date range slider for filtering
        selected_range = st.slider(
            "Select Date Range",
            min_value=min_date.date(),
            max_value=max_date.date(),
            value=(min_date.date(), max_date.date())
        )

        filtered_data = df[(df['Start_date'].dt.date >= selected_range[0]) & (df['Start_date'].dt.date <= selected_range[1])]

        # Display the subtitle with the selected range
        st.markdown(f"### Overall Time Range Selected: {selected_range[0]} to {selected_range[1]}")


        # Add a radio button to toggle between daily and monthly data
        time_frame = st.radio(
            "Select Time Frame:",
            ('Monthly', 'Daily')
        )

        # Group data by the selected time frame
        if time_frame == 'Monthly':
            grouped_data = filtered_data.groupby(by=filtered_data['Start_date'].dt.to_period('M'))['nb_rides'].sum().reset_index(name='Ride Count')
            grouped_data['Start_date'] = grouped_data['Start_date'].dt.to_timestamp()
        elif time_frame == 'Daily':
            grouped_data = filtered_data.groupby(by=filtered_data['Start_date'].dt.to_period('D'))['nb_rides'].sum().reset_index(name='Ride Count')
            grouped_data['Start_date'] = grouped_data['Start_date'].dt.to_timestamp()

        # Create a time series chart
        fig = px.line(grouped_data, x='Start_date', y='Ride Count',
                    title=f'Total Number of Rides - {time_frame}',
                    labels={'Start_date': 'Date', 'Ride Count': 'Number of Rides'})

        # Display the chart
        st.plotly_chart(fig,use_container_width=True,)

        # Create the plotly chart figure using the function
        bin_width_options = [4, 8, 12, 16]  # Define your own bin width options as needed.
        bin_width = st.sidebar.selectbox('Select bin width:', options=bin_width_options, index=0)
        hist_figure = create_histogram(df_hist, bin_width)

        # Display the chart in Streamlit
        st.plotly_chart(hist_figure, use_container_width=True)

        # Calculate top 10 districts
        top_district_start = df_district_start[(df_district_start['Start_date'].dt.date >= selected_range[0]) & (df_district_start['Start_date'].dt.date <= selected_range[1])]
        top_10_district_start = top_district_start.groupby(by="district_start")['nb_rides'].sum().sort_values(ascending=False).head(10)

        top_district_end = df_district_end[(df_district_end['Start_date'].dt.date >= selected_range[0]) & (df_district_end['Start_date'].dt.date <= selected_range[1])]
        top_10_district_end = top_district_end.groupby(by="district_end")['nb_rides'].sum().sort_values(ascending=False).head(10)


        # Create two columns for the charts
        col1, col2 = st.columns(2)

        # Display the top stations as a table in the first column
        with col1:
            st.subheader("Top 10 Starting Districts")
            st.dataframe(top_10_district_start)

        # Display the top routes as a DataFrame in the second column
        with col2:
            st.subheader("Top 10 Ending Districts")
            st.dataframe(top_10_district_end)


if __name__ == "__main__":

    dashboard = Bikesdashboard()
    dashboard.introduction_page()
