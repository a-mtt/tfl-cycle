import streamlit as st
from streamlit_keplergl import keplergl_static
from keplergl import KeplerGl
from map_dashboard.advanced.bigquery_helper import *
import os
import json

st.set_page_config(layout="wide")

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')
bq_client = create_bigquery_client(credentials_path=CREDENTIALS_PATH)

#@st.cache(ttl=600, show_spinner=True)
#def get_table_data(client, dataset_name, table_name):
#    return query_all_from_table(client, dataset_name, table_name)


st.title('Top routes Map')
st.write('The geographic arcs on this map display the top routes that bikes renters ride on. From each district spans an arc that displays the most common district destination.')

# Cache the query to prevent re-running it on each app interaction

table_data = query_map_one()

config ={
    "version": "v1",
    "config": {
        "visState": {
            "filters": [],
            "layers": [
                {
                    "id": "ywcmmh6",
                    "type": "arc",
                    "config": {
                        "dataId": "data_1",
                        "label": "new layer",
                        "color": [
                            255,
                            203,
                            153
                        ],
                        "highlightColor": [
                            252,
                            242,
                            26,
                            255
                        ],
                        "columns": {
                            "lat0": "lat_start",
                            "lng0": "lon_start",
                            "lat1": "lat_end",
                            "lng1": "lon_end"
                        },
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.8,
                            "thickness": 7.2,
                            "colorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#FFC300",
                                    "#F1920E",
                                    "#E3611C",
                                    "#C70039",
                                    "#900C3F",
                                    "#5A1846"
                                ],
                                "reversed": True
                            },
                            "sizeRange": [
                                0,
                                10
                            ],
                            "targetColor": None
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                "field": None,
                                "color": [
                                    255,
                                    255,
                                    255
                                ],
                                "size": 18,
                                "offset": [
                                    0,
                                    0
                                ],
                                "anchor": "start",
                                "alignment": "center"
                            }
                        ]
                    },
                    "visualChannels": {
                        "colorField": {
                            "name": "nb_rides",
                            "type": "integer"
                        },
                        "colorScale": "quantile",
                        "sizeField": None,
                        "sizeScale": "linear"
                    }
                }
            ],
            "interactionConfig": {
                "tooltip": {
                    "fieldsToShow": {
                        "data_1": [
                            {
                                "name": "district_start",
                                "format": None
                            },
                            {
                                "name": "district_end",
                                "format": None
                            },
                            {
                                "name": "nb_rides",
                                "format": None
                            },
                            {
                                "name": "rank",
                                "format": None
                            }
                        ]
                    },
                    "compareMode": False,
                    "compareType": "absolute",
                    "enabled": True
                },
                "brush": {
                    "size": 0.5,
                    "enabled": False
                },
                "geocoder": {
                    "enabled": False
                },
                "coordinate": {
                    "enabled": False
                }
            },
            "layerBlending": "normal",
            "splitMaps": [],
            "animationConfig": {
                "currentTime": None,
                "speed": 1
            }
        },
        "mapState": {
            "bearing": 24,
            "dragRotate": True,
            "latitude": 51.45760051693673,
            "longitude": -0.14506746719368874,
            "pitch": 50,
            "zoom": 11.766363092491126,
            "isSplit": False
        },
        "mapStyle": {
            "styleType": "dark",
            "topLayerGroups": {},
            "visibleLayerGroups": {
                "label": True,
                "road": True,
                "border": False,
                "building": True,
                "water": True,
                "land": True,
                "3d building": False
            },
            "threeDBuildingColor": [
                9.665468314072013,
                17.18305478057247,
                31.1442867897876
            ],
            "mapStyles": {}
        }
    }
}

map_1 = KeplerGl(height=800, data={'data_1': table_data},config=config)
#map_1.add_data(data=table_data, name='data_1')
keplergl_static(map_1)

st.write("This is a kepler.gl map in streamlit")
