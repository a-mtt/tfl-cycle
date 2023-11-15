import numpy as np
import plotly.express as px
import pandas as pd

# Now, using the DataFrame `df`, we create the histogram with Plotly.
def create_histogram(dataframe, bin_width):
    fig = px.histogram(dataframe, x='duration_min_rounded', y='nb_rides', nbins=int(dataframe['duration_min_rounded'].max() / bin_width))
    fig.update_layout(
        title='Histogram of Ride Durations',
        xaxis_title='Duration Rounded (min)',
        yaxis_title='Number of Rides',
        bargap=0.2
    )
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
