import pandas as pd
import geopandas as gpd
import colorsys
import folium
from folium import plugins
import logging

from folium.plugins import HeatMap
from matplotlib import pyplot as plt

from db_connector import RemoteDB
import shapely
from shapely import wkb
import json

## MUST IMPORT otherwise contains the functions used in db interaction
from db_utils import *

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

speedLimits = ["T0","T20","T30","T50","T60","T80","T100"]
color_dict = {
    "T0": "red",
    "T20": "orange",
    "T30": "green",
    "T50": "yellow",
    "T60": "purple",
    "T80": "pink",
    "T100": "gray"
}


# Create Maps =========================================================================================================
def create_heat_map_with_time(folium_map):

    # Process heat map data
    heat_view_data = get_view("heat")
    heat_df = gpd.GeoDataFrame(heat_view_data, columns=['latitude', 'longitude', 'year'])

    assert not heat_df.empty, f" Heat Dataframe is empty: {heat_df.head(5)}"
    add_heat_map_time(heat_df, folium_map)
    logger.info(f"Heat map time added to time map.")
    #interactive_map.save("test.html")

    add_signaled_speeds(folium_map)

    # Add bikes

    add_bike_heat_map_time(folium_map)

    #Pedestrian Part

    add_pedestrian_heat_map_time(folium_map)

    folium.LayerControl(collapsed=True).add_to(folium_map)


def create_heat_map_toggle(folium_map):

    heat_view_data = get_view("heat")
    heat_gdf = gpd.GeoDataFrame(heat_view_data, columns=['latitude', 'longitude', 'year'])

    assert not heat_gdf.empty, f" Heat Dataframe is empty: {heat_gdf.head(5)}"

    add_heat_year_toggle(heat_gdf, folium_map)

    add_bike_heat_toggle(folium_map)
    add_ped_heat_map(folium_map)
    # Add signald speeds data
    add_signaled_speeds(folium_map)

    folium.LayerControl(collapsed=True).add_to(folium_map)


# Layer Adding Methods ================================================================================================
def add_bike_heat_map_time(folium_map):

    # Process heat map data
    bike_heat_view_data = get_view('bikeheat', 'latitude, longitude, year')
    bike_heat_df = gpd.GeoDataFrame(bike_heat_view_data, columns=['latitude', 'longitude', 'year'])

    assert not bike_heat_df.empty, f" Heat Dataframe is empty: {bike_heat_df.head(5)}"
    heat_data = [[[row['latitude'], row['longitude'], 0.1] for index, row in bike_heat_df[bike_heat_df['year'] == i].iterrows()] for
                 i in range(2011, 2023)]
    logger.debug(f"First element of heat data: {heat_data[0]}")
    index = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    AccidentType = "Bicycles: "
    index = [str(element) for element in index]
    index = [AccidentType + element for element in index]
    # plot heat map
    gradient = generate_hue_gradient(0.6, 5)
    hm = plugins.HeatMapWithTime(heat_data,
                                 auto_play=False,
                                 max_opacity=1,
                                 gradient=gradient,
                                 min_opacity=0.5,
                                 radius=9,
                                 use_local_extrema=False,
                                 blur=1,
                                 index=index,
                                 name="Accident Heatmap Bikes")
    hm.add_to(folium_map)


def add_pedestrian_heat_map_time(folium_map):

    # Process heat map data
    pedestrian_heat_view_data = get_view("pedestrianheat")
    heat_df = gpd.GeoDataFrame(pedestrian_heat_view_data, columns=['latitude', 'longitude', 'year'])

    assert not heat_df.empty, f" Heat Dataframe is empty: {heat_df.head(5)}"
    heat_data = [[[row['latitude'], row['longitude'], 0.5] for index, row in heat_df[heat_df['year'] == i].iterrows()] for
                 i in range(2011, 2023)]
    logger.debug(f"First element of PED heat data: {heat_data[0]}")
    index = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    AccidentType = "Pedestrians: "
    index = [str(element) for element in index]
    index = [AccidentType + element for element in index]
    #gradient =
    # plot heat map
    gradient = generate_hue_gradient(0.2, 5)
    hm = plugins.HeatMapWithTime(heat_data,
                                 auto_play=False,
                                 max_opacity=1,
                                 gradient=gradient,
                                 min_opacity=0.5,
                                 radius=9,
                                 use_local_extrema=False,
                                 blur=1,
                                 index=index,
                                 name="Accident Heatmap Pedestrian")
    hm.add_to(folium_map)


def add_heat_map_time(heat_df, folium_map):
    heat_data = [[[row['latitude'], row['longitude'], 0.5] for index, row in heat_df[heat_df['year'] == i].iterrows()] for
                 i in range(2011, 2023)]
    index = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    # create heat map
    logger.debug(f"First element of heat data: {heat_data[0]}")
    hm = plugins.HeatMapWithTime(heat_data,
                                 auto_play=False,
                                 max_opacity=0.8,
                                 gradient=gradient,
                                 min_opacity=0.3,
                                 radius=9,
                                 use_local_extrema=False,
                                 blur=1,
                                 index=index,
                                 name="Accident Heatmap ALL")
    hm.add_to(folium_map)


def add_signaled_speeds(folium_map):
    # Add signald speeds data
    rows = """
            temporegime_technical as tempo, 
            wkb_geometry
    """
    sig_speeds_data = get_view("signaled_speeds", rows)
    sig_speed_df = pd.DataFrame(sig_speeds_data, columns=['tempo', 'wkb_geometry'])
    sig_speed_df['geometry'] = sig_speed_df['wkb_geometry'].apply(lambda x: wkb.loads(x, hex=True))
    logger.debug(f"{sig_speed_df.head()}")
    sig_speed_gdf = gpd.GeoDataFrame(sig_speed_df, geometry="geometry")
    for speedlimit in speedLimits:
        signal_speed = sig_speed_gdf[sig_speed_gdf["tempo"].str.contains(speedlimit, case=False)]
        geometries = json.loads(json.dumps(shapely.geometry.mapping(signal_speed['geometry'].unary_union)))

        folium.GeoJson(
            data=geometries,
            name=f'Signaled Speed {speedlimit}',
            color=color_dict[speedlimit],
            show=False,
            line_cap="butt",
        ).add_to(folium_map)


def add_heat_year_toggle(heat_gdf, folium_map, name="All"):
    index = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    # plot heat map
    for year in index:
        year_data = heat_gdf[heat_gdf['year'] == year]

        heatmap_layer = HeatMap(
            data=year_data[['latitude', 'longitude']],
            radius=8,
            gradient=gradient,
            min_opacity=0.5,
            max_opacity=0.8,
            blur=10,
            show=False,
            name=f'Accidents involving {name} in {year}'
        )

        heatmap_layer.add_to(folium_map)


def add_bike_heat_toggle(folium_map):
    bike_heat_view_data = get_view('bikeheat', 'latitude, longitude, year')
    heat_gdf = gpd.GeoDataFrame(bike_heat_view_data, columns=['latitude', 'longitude', 'year'])
    add_heat_year_toggle(heat_gdf, folium_map, name="motorcycles")


def add_ped_heat_map(folium_map):
    pedestrian_heat_view_data = get_view("pedestrianheat")
    heat_gdf = gpd.GeoDataFrame(pedestrian_heat_view_data, columns=['latitude', 'longitude', 'year'])
    add_heat_year_toggle(heat_gdf, folium_map, name="pedestrians")


# Utilities ===========================================================================================================

def save_map_as_html(folium_map, name):
    folium_map.save(f"{name}.html")
    logger.info(f"Succesfully saved map {name}.")


def setup_views():
    drop_view("heat")
    create_heat_view()
    drop_view("bikeheat")
    create_bike_heat_view()
    drop_view("pedestrianheat")
    create_pedestrian_heat_view()


def generate_hue_gradient(hue, num_colors):
    if num_colors < 2:
        num_colors = 2
    gradient = {}
    for i in range(num_colors):
        lightness = 0.1 + 0.8 * (i / (num_colors - 1))
        saturation = 0.1 + 0.8 * (i / (num_colors - 1))
        rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
        gradient[i / (num_colors - 1)] = '#{:02x}{:02x}{:02x}'.format(int(rgb[0]*255), int(rgb[1]*255), int(rgb[2]*255))
    return gradient

def generate_contrasting_gradient(num_colors):
    cmap = plt.get_cmap('viridis')      # viridis is a map with contrasting colors
    gradient = {}
    for i in range(num_colors):
        rgba = cmap(i / (num_colors - 1))
        gradient[i / (num_colors - 1)] = '#{:02x}{:02x}{:02x}'.format(int(rgba[0]*255), int(rgba[1]*255), int(rgba[2]*255))
    return gradient



if __name__ == "__main__":
    time_map = folium.Map(
        location=zurich_coordinates,
        zoom_start=13,
        zoom_control=True,
        dragging=True,
        scrollWheelZoom=True,
        doubleClickZoom=False,
        tiles="cartodb positron"
    )

    toggle_map = folium.Map(
        location=zurich_coordinates,
        zoom_start=13,
        zoom_control=True,
        dragging=True,
        scrollWheelZoom=True,
        doubleClickZoom=False,
        tiles="cartodb positron"
    )

    #setup_views()

    create_heat_map_with_time(time_map)
    create_heat_map_toggle(toggle_map)

    ## Save Maps ============================================================================================
    save_map_as_html(toggle_map, "heat_map_toggle")
    save_map_as_html(time_map, "heat_map_time")
