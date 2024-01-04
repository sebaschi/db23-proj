import pandas as pd
import geopandas as gpd
import os
import folium
from folium import plugins
import logging
from db_connector import RemoteDB

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

def create_heat_view():
    create_heat_view_sql = """
        CREATE VIEW heat AS
    SELECT
        ST_Y(geometry) AS latitude,
        ST_X(geometry) AS longitude,
        AccidentYear AS Weight
    FROM
        accidents
    WHERE
        ST_Y(geometry) IS NOT NULL AND
        ST_X(geometry) IS NOT NULL AND
        AccidentYear IS NOT NULL;
    """

    remote_db = RemoteDB()
    remote_db.execute_query(create_heat_view_sql)
    remote_db.close()
    logger.info("Heat View Created")\

def get_heat_view():
    create_heat_view()

    get_heat_view_sql = """
    SELECT latitude, longitude, weight
    FROM heat;
    """

    remote_db = RemoteDB()

    # Get heat map data from database
    try:
        result = remote_db.execute_query(get_heat_view_sql)
        logger.info(f"Succesfully retrieved result {result}")
        return result
    except Exception as e:
        logger.exception(f"Failed getting result with exception {e}")
    finally:
        remote_db.close()

def create_acc_map():


    # Process heat map data
    pass
    #heat_df = pd.DataFrame(result, columns=['latitude', 'longitude', 'weight'])

