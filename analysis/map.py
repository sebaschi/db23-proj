import pandas as pd
import geopandas as gpd
import os
import folium
from folium import plugins
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('map.py')
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


accidents_filepath = "../src/datasets/integrated/Accidents.geojson"
signaled_speeds_filepath = "../src/datasets/integrated/signaled_speeds.geojson.geojson"

# Map centered around zurich
zurich_coordinates = [47.368650,  	8.539183]
fixed_map_zurich_original_coords = folium.Map(
    location=zurich_coordinates,
    zoom_start=13,
    zoom_control=False,
    dragging=False,
    scrollWheelZoom=False,
    doubleClickZoom=False
)
def create_acc_map():
    acc_gdf = gpd.read_file(accidents_filepath)
    acc_gdf['latitude'] = acc_gdf.geometry.y
    acc_gdf['longitude'] = acc_gdf.geometry.x


    # Ensure we're dealing with floats
    acc_gdf['latitude'] = acc_gdf['latitude'].astype(float)
    acc_gdf['longitude'] = acc_gdf['longitude'].astype(float)

    # Build heat dataframe used for mapping
    heat_df = acc_gdf
    heat_df = heat_df[['latitude', 'longitude']]
    heat_df = heat_df.dropna(axis=0, subset=['latitude', 'longitude'])
    heat_df = heat_df.dropna(axis=0, subset=['latitude', 'longitude'])
