import numpy as np
import plotly.express as px
import pandas as pd

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


### DEFINING HELPER FUNCTIONS

def clean_column_names(df):
    # Strip whitespace, lowercase, and replace spaces with underscores for each column name
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def extract_neighborhood(df, column_name, neighborhood_column_name):
    # Split the column on the comma and take the second part (strip to remove leading/trailing spaces)
    df[neighborhood_column_name] = df[column_name].str.split(',').str[1].str.strip()
    return df
