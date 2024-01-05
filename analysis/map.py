import pandas as pd
import geopandas as gpd
import os
import folium
from folium import plugins
import logging
from db_connector import RemoteDB
import shapely
from shapely import wkb
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

gradient = {
    0.1: 'blue',
    0.3: 'cyan',
    0.5: 'lime',
    0.7: 'yellow',
    0.9: 'red'
}

interactive_map = folium.Map(
    location=zurich_coordinates,
    zoom_start=13,
    zoom_control=True,
    dragging=True,
    scrollWheelZoom=True,
    doubleClickZoom=False,
    tiles="cartodb positron"
)

speedLimits = ["T0","T10","T20","T30","T50","T60","T80","T100"]
color_dict = {
    "T0": "red",
    "T10": "blue",
    "T20": "orange",
    "T30": "green",
    "T50": "yellow",
    "T60": "purple",
    "T80": "pink",
    "T100": "gray"
}

def drop_heat_view():
    drop_heat_view_sql = """
    DROP VIEW IF EXISTS heat;
    """

    remote_db = RemoteDB()
    try:
        result = remote_db.execute_query(drop_heat_view_sql)
        logger.info("Heat View dropped.")
    except Exception as e:
        logger.exception(f"Exception while dropping heat view. Msg: {e} ")
    finally:
        remote_db.close()
        logger.debug(f"RemoteDB object closed.")



def create_heat_view():
    create_heat_view_sql = """
        CREATE VIEW heat AS
    SELECT
        ST_Y(geometry) AS latitude,
        ST_X(geometry) AS longitude,
        AccidentYear AS year
    FROM
        accidents
    WHERE
        ST_Y(geometry) IS NOT NULL AND
        ST_X(geometry) IS NOT NULL AND
        AccidentYear IS NOT NULL;
    """

    remote_db = RemoteDB()
    remote_db.execute_command(create_heat_view_sql)
    remote_db.close()
    logger.info("Heat View Created")\

def get_heat_view():
    get_heat_view_sql = """
    SELECT latitude, longitude, year
    FROM heat;
    """

    remote_db = RemoteDB()

    # Get heat map data from database
    try:
        result = remote_db.execute_query(get_heat_view_sql)
        logger.info(f"Succesfully retrieved result")
        return result
    except Exception as e:
        logger.exception(f"Failed getting result with exception {e}")
    finally:
        remote_db.close()

def create_heat_map_with_time():


    # Process heat map data
    heat_view_data = get_heat_view()
    heat_df = gpd.GeoDataFrame(heat_view_data, columns=['latitude', 'longitude', 'year'])

    assert not heat_df.empty, f" Heat Dataframe is empty: {heat_df.head(5)}"
    heat_data = [[[row['latitude'], row['longitude']] for index, row in heat_df[heat_df['year'] == i].iterrows()] for
                 i in range(2011, 2023)]

    index = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    # plot heat map
    hm = plugins.HeatMapWithTime(heat_data,
                                 auto_play=False,
                                 max_opacity=0.8,
                                 index=index,
                                 name="Accident Heatmap")
    hm.add_to(interactive_map)
    interactive_map.save("test.html")

    # Add signald speeds data
    sig_speeds_data = get_signaled_speed_sql()
    sig_speed_df = pd.DataFrame(sig_speeds_data, columns=['tempo','wkb_geometry'])
    sig_speed_df['geometry'] = sig_speed_df['wkb_geometry'].apply(lambda x: wkb.loads(x, hex=True))
    logger.debug(f"{sig_speed_df.head()}")
    sig_speed_gdf = gpd.GeoDataFrame(sig_speed_df, geometry="geometry")

    for speedlimit in speedLimits:
        signal_speed = sig_speed_gdf[sig_speed_gdf["tempo"].str.contains(speedlimit, case=False)]
        geometries = json.loads(json.dumps(shapely.geometry.mapping(signal_speed['geometry'].unary_union)))

        folium.GeoJson(
            data=geometry,
            name=f'Signaled Speed {speedlimit}',
            color=color_dict[speedlimit],
            show=False,
            line_cap="butt",
        ).add_to(interactive_map)

    folium.LayerControl(collapsed=True).add_to(interactive_map)
    interactive_map.save("test.html")


def get_signaled_speed_sql():
    sigspeed_sql = """
    SELECT 
        temporegime_technical as tempo, 
        wkb_geometry
    FROM signaled_speeds;
    """
    remote_db = RemoteDB()
    try:
        result = remote_db.execute_query(sigspeed_sql)
        logger.info(f"Succesfully retrieved result")
        return result
    except Exception as e:
        logger.exception(f"Failed getting result with exception {e}")
    finally:
        remote_db.close()


def save_map_as_html(map, name):
    map.save(f"{name}.html")
    logger.info(f"Succesfully saved map {name}.")


if __name__ == "__main__":
    drop_heat_view()
    create_heat_view()
    create_heat_map_with_time()
    save_map_as_html(interactive_map, "heat_map_with_time")