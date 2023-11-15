import streamlit as st
from streamlit_keplergl import keplergl_static
from keplergl import KeplerGl
from map_dashboard.advanced.bigquery_helper import *
import os
import json

st.set_page_config(layout="wide")

CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH')
bq_client = create_bigquery_client(credentials_path=CREDENTIALS_PATH)

st.title('Daily Seasonality Map')
st.write('This interactive map showcases the daily seasonality patterns. Each volume represents bikes that are being taken during the selected timeframe')

# Cache the query to prevent re-running it on each app interaction

table_data = query_map_two()

config =  {
    "version": "v1",
    "config": {
        "visState": {
            "filters": [
                {
                    "dataId": [
                        "data_1"
                    ],
                    "id": "64z82m49",
                    "name": [
                        "date"
                    ],
                    "type": "timeRange",
                    "value": [
                        -2208963657792.007,
                        -2208953819792.007
                    ],
                    "enlarged": True,
                    "plotType": "histogram",
                    "animationWindow": "free",
                    "yAxis": None,
                    "speed": 4.992
                }
            ],
            "layers": [
                {
                    "id": "su7zcjh",
                    "type": "grid",
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
                            "lat": "lat_start",
                            "lng": "lon_start"
                        },
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.8,
                            "worldUnitSize": 0.4,
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
                            "coverage": 1,
                            "sizeRange": [
                                0,
                                500
                            ],
                            "percentile": [
                                0,
                                100
                            ],
                            "elevationPercentile": [
                                0,
                                100
                            ],
                            "elevationScale": 10,
                            "enableElevationZoomFactor": True,
                            "colorAggregation": "average",
                            "sizeAggregation": "average",
                            "enable3d": True
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
                        "sizeField": {
                            "name": "nb_rides",
                            "type": "integer"
                        },
                        "sizeScale": "linear"
                    }
                }
            ],
            "interactionConfig": {
                "tooltip": {
                    "fieldsToShow": {
                        "data_1": [
                            {
                                "name": "hour",
                                "format": None
                            },
                            {
                                "name": "minute",
                                "format": None
                            },
                            {
                                "name": "nb_rides",
                                "format": None
                            },
                            {
                                "name": "date",
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
            "latitude": 51.566643340250934,
            "longitude": -0.04851443212952115,
            "pitch": 50,
            "zoom": 10.020420224058581,
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

map_1 = KeplerGl(height=800, data={'data_1': table_data}, config=config)
keplergl_static(map_1)

st.write("This is a kepler.gl map in streamlit")
