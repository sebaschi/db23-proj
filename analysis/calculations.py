import logging
from shapely.geometry import Point, LineString
from shapely import wkb
from shapely.geometry import shape
from db_connector import RemoteDB
import pandas as pd
from pyproj import Proj, transform
from geopy.distance import geodesic

speedLimits = ["T0", "T20", "T30", "T50","T60", "T80", "T100"]


def is_point_near_multilinestring(point, multilinestring, threshold_distance):
    point_geometry = Point(point)
    return point_geometry.distance(multilinestring) < threshold_distance


def get_data(db):
    get_speeds_sql = """
    SELECT wkb_geometry,
       temporegime_technical
    FROM signaled_speeds;
    """

    result = db.execute_query(get_speeds_sql)
    sig_speed_df = pd.DataFrame(result)
    sig_speed_df.rename(columns={'wkb_geometry': 'geometry'}, inplace=True)
    sig_speed_df['geometry'] = sig_speed_df['geometry'].apply(lambda x: wkb.loads(x, hex=True))

    get_accidents = """
    SELECT geometry
    FROM accidents;
    """
    result = db.execute_query(get_accidents)
    accident_df = pd.DataFrame(result)
    accident_df['geometry'] = accident_df['geometry'].apply(lambda x: wkb.loads(x, hex=True))

    process_data(sig_speed_df, accident_df)


def process_data(sig_speed_df, accident_df):
    result_df = pd.DataFrame(columns= ['TempoLim', 'Accidents_total', ])
    for speed in speedLimits:
        print("Checking for zone: " + speed)
        filtered_df = sig_speed_df[sig_speed_df["temporegime_technical"].str.contains(speed, case=False, na=False)]
        current_result = count_points_near_multilinestrings(accident_df, filtered_df, 0.00001)
        result_df.loc[len(result_df)] = {'TempoLim': speed, 'Accidents_total': current_result}
    print("FINAL RESULT")
    print(result_df)


def count_points_near_multilinestrings(points_df, multilinestrings_df, threshold_distance):
    result_counts = []

    for idx, multilinestring_row in multilinestrings_df.iterrows():
        multilinestring = multilinestring_row['geometry']
        count_near = sum(points_df['geometry'].apply(
            lambda point: is_point_near_multilinestring(point, multilinestring, threshold_distance)))
        result_counts.append({'temporegime_technical': multilinestring_row['temporegime_technical'], 'CountNear': count_near})
    result_df = pd.DataFrame(result_counts)
    return result_df['CountNear'].sum()

def calculate_sigspeed_length(db):
    for speed in speedLimits:
        get_data_sql = f"""
        SELECT wkb_geometry, temporegime_technical
        FROM signaled_speeds
        WHERE temporegime_technical = '{speed}';
        """

        result = db.execute_query(get_data_sql)
        result_df = pd.DataFrame(result)
        result_df['wkb_geometry'] = result_df['wkb_geometry'].apply(lambda x: wkb.loads(x, hex=True))
        sigspeed_length = result_df['wkb_geometry'].apply(lambda x:get_accumulated_distance(x)).sum()
        sigspeed_length = str(round(sigspeed_length * 1000, 2)) + " km"
        print("Length for " + speed + ": " + sigspeed_length)

def get_accumulated_distance(coords_str):
    polyline_geometry = shape(coords_str)
    return polyline_geometry.length

if __name__ == "__main__":
    remote_db = RemoteDB()

    try:
        #get_data(remote_db)
        calculate_sigspeed_length(remote_db)
    except Exception as e:
        print(f"Exception {e} in calculations.py")
    finally:
        remote_db.close()
