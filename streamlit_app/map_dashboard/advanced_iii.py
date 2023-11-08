import streamlit as st
st.set_page_config(layout="wide")
from map_dashboard.advanced.bigquery_helper import create_bigquery_client, query_all_from_table
import os
import numpy as np
import plotly.express as px
import pandas as pd

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')

bq_client = create_bigquery_client(credentials_path=CREDENTIALS_PATH)
table_data = query_all_from_table(CREDENTIALS_PATH, dataset_name=DATASET_NAME, table_name=TABLE_NAME)

df = table_data

# Convert 'Start_date' to datetime

df['Start_date'] = pd.to_datetime(df['Start_date'])

# Get the minimum and maximum dates from the DataFrame

min_date = df['Start_date'].min()
max_date = df['Start_date'].max()

def create_duration_distribution_chart(data, duration_col='Total_duration__ms_'):
    """
    Create a distribution chart of ride durations in minutes.

    Parameters:
    - data: DataFrame containing the bike rides data
    - duration_col: the name of the column containing duration in milliseconds

    Returns:
    - A Plotly figure object
    """
    # Convert 'Total_duration_ms' from milliseconds to minutes in a separate Series
    duration_minutes = data[duration_col] / (1000 * 60)

    # Create bins of 4-minute intervals for the 'Duration_minutes' Series
    bin_width = 4
    bins = np.arange(0, duration_minutes.max() + bin_width, bin_width)
    duration_bins = pd.cut(duration_minutes, bins=bins, include_lowest=True)

    # Calculate the distribution of ride durations within the bins
    duration_distribution = duration_bins.value_counts().sort_index()

    # Create a DataFrame from the distribution for plotting
    distribution_df = pd.DataFrame({
        'Duration Bins (minutes)': [f'{int(bin.left)}-{int(bin.right)}' for bin in duration_distribution.index],
        'Number of Rides': duration_distribution.values
    })

    # Create the chart
    fig = px.bar(
        distribution_df,
        x='Duration Bins (minutes)',
        y='Number of Rides',
        labels={'Duration Bins (minutes)': 'Duration (minutes)', 'Number of Rides': 'Number of Rides'},
        title='Distribution of Ride Durations'
    )

    # Update x-axis tick angle for better readability
    fig.update_layout(xaxis_tickangle=-45)

    return fig

class Bikesdashboard:
    def __init__(self) -> None:
        pass

    def introduction_page(self):
        """Layout the views of the dashboard"""

        st.title("London Bikes Dashboard")


        # Sidebar: Selection of bike model
        bike_model = st.sidebar.multiselect(
            'Select Bike Model:',
            options=df['Bike_model'].unique(),
            default=df['Bike_model'].unique()
        )

        # Sidebar: Selection of Start Station
        start_station = st.sidebar.multiselect(
            'Select Start Station:',
            options=df['Start_station'].unique(),
            default=df['Start_station'].unique()
        )

        # Filter data based on sidebar selection
        filtered_data = df[(df['Bike_model'].isin(bike_model)) & (df['Start_station'].isin(start_station))]

        # Main panel

        # Custom HTML and CSS to center the title
        st.markdown("""
            <style>
            .title {
                text-align: center;
            }
            </style>
            <h1 class="title">Your Page Title</h1>
        """, unsafe_allow_html=True)

        # Add a date range slider for filtering
        selected_range = st.slider(
            "Select Date Range",
            min_value=min_date.date(),
            max_value=max_date.date(),
            value=(min_date.date(), max_date.date())
        )

        filtered_data = filtered_data[(filtered_data['Start_date'].dt.date >= selected_range[0]) & (filtered_data['Start_date'].dt.date <= selected_range[1])]

        # Display the subtitle with the selected range
        st.markdown(f"### Overall Time Range Selected: {selected_range[0]} to {selected_range[1]}")


        # Add a radio button to toggle between daily and monthly data
        time_frame = st.radio(
            "Select Time Frame:",
            ('Monthly', 'Daily')
        )

        # Group data by the selected time frame
        if time_frame == 'Monthly':
            grouped_data = filtered_data.groupby(filtered_data['Start_date'].dt.to_period('M')).size().reset_index(name='Ride Count')
            grouped_data['Start_date'] = grouped_data['Start_date'].dt.to_timestamp()
        elif time_frame == 'Daily':
            grouped_data = filtered_data.groupby(filtered_data['Start_date'].dt.to_period('D')).size().reset_index(name='Ride Count')
            grouped_data['Start_date'] = grouped_data['Start_date'].dt.to_timestamp()

        # Create a time series chart
        fig = px.line(grouped_data, x='Start_date', y='Ride Count',
                    title=f'Total Number of Rides - {time_frame}',
                    labels={'Start_date': 'Date', 'Ride Count': 'Number of Rides'})

        # Display the chart
        st.plotly_chart(fig,use_container_width=True,)

        # Create the plotly chart figure using the function
        duration_chart = create_duration_distribution_chart(filtered_data, 'Total_duration__ms_')

        # Display the chart in Streamlit
        st.plotly_chart(duration_chart, use_container_width=True)

        # Calculate top 10 stations
        top_stations = filtered_data['Start_station'].value_counts().head(10)

        # Calculate top 10 routes
        filtered_data['Route'] = filtered_data['Start_station'] + ' to ' + filtered_data['End_station']
        top_routes = filtered_data['Route'].value_counts().head(10)

        # Create two columns for the charts
        col1, col2 = st.columns(2)

        # Create two columns for the tables using the `columns` function
        col1, col2 = st.columns(2)

        # Display the top stations as a table in the first column
        with col1:
            st.subheader("Top 10 Starting Stations")
            st.dataframe(top_stations)

        # Display the top routes as a DataFrame in the second column
        with col2:
            st.subheader("Top 10 Routes")
            st.dataframe(top_routes)


if __name__ == "__main__":

    dashboard = Bikesdashboard()
    dashboard.introduction_page()
