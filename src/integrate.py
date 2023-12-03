import data_utils as du
import os
import pandas as pd
import geopandas as gpd
import time
from shapely.geometry import Point
import re

import logging

logging.basicConfig(level=logging.DEBUG, filename='logs/integrate.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('integrate.py')
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

foot_bike_urls_file = '../docs/foot_bike_zaehlung_urls.txt'
miv_file_urls = '../docs/verkehrszaehlung_moto_urls.txt'
accident_file_url = '../docs/accident_loc_urls.txt'

# Using u_string to discriminate between files that belong to each other
motor_file_u_string = 'sid_dav_verkehrszaehlung_miv_OD2031'
foot_bike_file_u_string = 'velo.csv'
accident_file_u_string = 'RoadTrafficAccidentLocations.json'

data_dir = 'datasets/'
integrated_dir = 'datasets/integrated/'
logs_dir = 'logs/'

weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

fb_data_types = {
    'ID': 'int',
    'NORD': 'int',
    'OST': 'int',
    'DATE': 'str',
    'HRS': 'int',
    'VELO_IN': 'int',
    'VELO_OUT': 'int',
    'FUSS_IN': 'int',
    'FUSS_OUT': 'int',
    'Weekday_en': 'str'
}

miv_data_types = {
    'ID': 'int',
    'MSID': 'str',
    'ZSID': 'str',
    'Achse': 'str',
    'NKoord': 'int',
    'EKoord': 'int',
    'Richtung': 'str',
    'AnzFahrzeuge': 'int',
    'AnzFahrzeugeStatus': 'str',
    'Datum': 'str',
    'Hrs': 'int',
    'Weekday_en': 'str',
}

acc_data_types = {
    'AccidentUID': 'str',
    'AccidentYear': 'int',
    'AccidentMonth': 'int',
    'AccidentWeekDay_en': 'str',
    'AccidentHour': 'int',
    'NKoord': 'int',
    'EKoord': 'int',
    'AccidentType_en': 'str',
    'AccidentType': 'str',
    'AccidentSeverityCategory': 'str',
    'AccidentInvolvingPedestrian': 'bool',
    'AccidentInvolvingBicycle': 'bool',
    'AccidentInvolvingMotorcycle': 'bool',
    'RoadType': 'str',
    'RoadType_en': 'str',
    'geometry': 'str' # TODO: Figure out what dtype this needs to be for postgres
}


def ensure_dirs_exist(data_dir, integrated_dir):
    """
    This should be called before anything else to make sure that the relevant directories exists.
    :param data_dir: directory where the datasets are stored
    :param integrated_dir: directory where the integrated data will be stored
    :return:
    """
    logger.debug(f'data_dir: {data_dir}\n integrated_dir: {integrated_dir}')
    logger.info("Ensuring needed directories exist.")
    os.makedirs(data_dir, exist_ok=True)
    logger.debug("data_dir created.")
    os.makedirs(integrated_dir, exist_ok=True)
    logger.debug("integrated_dir created")
    os.makedirs(logs_dir, exist_ok=True)
    logger.debug("logs_dir created")


def process_foot_bike_data(files_present=True):
    fb_df_unified = du.create_unified_df(foot_bike_urls_file, foot_bike_file_u_string, data_dir,
                                         files_present=files_present)
    fb_df_unified[['DATE', "TIME"]] = fb_df_unified['DATUM'].str.split('T', expand=True)
    fb_df_unified[['HRS', 'MINS']] = fb_df_unified['TIME'].str.split(':', expand=True)
    ## Evt brauchen wir doch FK_ZAEHLER
    fb_cols_to_drop = ['DATUM']
    fb_df_unified_correct_cols = fb_df_unified.drop(columns=fb_cols_to_drop, axis=1)
    fb_df_unified_correct_cols.fillna(0, inplace=True)
    fb_df_grouped = fb_df_unified_correct_cols.groupby(['OST', 'NORD', 'DATE', 'HRS']).agg({
        'VELO_IN': 'sum',
        'VELO_OUT': 'sum',
        'FUSS_IN': 'sum',
        'FUSS_OUT': 'sum'
    }).reset_index()
    dt_obj = pd.to_datetime(fb_df_grouped['DATE'])
    days = dt_obj.dt.weekday
    fb_df_grouped['Weekday_en'] = days.map(lambda x: weekday_names[x])
    cleaned_fb_df = fb_df_grouped
    cleaned_fb_df['ID'] = cleaned_fb_df.index + 1
    cleaned_fb_df = cleaned_fb_df[['ID', 'NORD', 'OST', 'DATE', 'HRS', 'VELO_IN', 'VELO_OUT', 'FUSS_IN',
                                   'FUSS_OUT', 'Weekday_en']]
    # Ensure datatype of df and sql table match
    cleaned_fb_df = cleaned_fb_df.astype(fb_data_types)
    return cleaned_fb_df


def process_miv_data(files_present=True):
    miv_df_unified = du.create_unified_df(miv_file_urls, motor_file_u_string, data_dir, files_present=files_present)

    miv_df_unified[['Datum', "Time"]] = miv_df_unified['MessungDatZeit'].str.split('T', expand=True)
    miv_df_unified[['Hrs', 'Mins', 'Sec']] = miv_df_unified['Time'].str.split(':', expand=True)

    miv_cols_to_keep = ['MSID','ZSID','Achse', 'NKoord', 'EKoord',  'Richtung', 'AnzFahrzeuge', 'AnzFahrzeugeStatus',
                        'Datum', 'Hrs',]
    miv_df_cols_dropped = miv_df_unified[miv_cols_to_keep]

    dt_obj = pd.to_datetime(miv_df_cols_dropped['Datum'])
    days = dt_obj.dt.weekday
    miv_df_cols_dropped.loc[:, 'Weekday_en'] = days.map(lambda x: weekday_names[x])

    miv_df_cols_dropped.loc[:, 'AnzFahrzeuge'] = miv_df_cols_dropped['AnzFahrzeuge'].fillna(0).astype(int)
    miv_df_cols_dropped[:, 'ZSID'] = miv_df_cols_dropped['ZSID'].fillna('Missing').astype(str)
    miv_df_cols_dropped['ID'] = (miv_df_cols_dropped.index + 1).copy()

    cleaned_miv_df = miv_df_cols_dropped[['ID', 'MSID', 'ZSID', 'Achse', 'NKoord', 'EKoord', 'Richtung', 'AnzFahrzeuge',
                                          'AnzFahrzeugeStatus', 'Datum', 'Hrs', 'Weekday_en']]

    cleaned_miv_df = cleaned_miv_df.astype(miv_data_types)
    cleaned_miv_df = cleaned_miv_df.drop_duplicates()
    return cleaned_miv_df


def process_accident_data(file_present: bool = True):
    if not file_present:
        du.process_urls(data_dir, accident_file_url)
    acc_df_unified = du.load_dataframes_from_geojson_files(data_dir, accident_file_u_string)
    acc_cols_to_keep = ['AccidentUID', 'AccidentYear', 'AccidentMonth', 'AccidentWeekDay_en','AccidentHour',
                        'AccidentLocation_CHLV95_N', 'AccidentLocation_CHLV95_E', 'AccidentType_en', 'AccidentType',
                        'AccidentSeverityCategory', 'AccidentInvolvingPedestrian', 'AccidentInvolvingBicycle',
                        'AccidentInvolvingMotorcycle', 'RoadType', 'RoadType_en',
                        'geometry']
    cleaned_acc_df = acc_df_unified[acc_cols_to_keep]
    cleaned_acc_df.rename(columns={
        'AccidentLocation_CHLV95_E': 'EKoord',
        'AccidentLocation_CHLV95_N': 'NKoord',
    }, inplace=True)

    cleaned_acc_df = cleaned_acc_df.astype(acc_data_types)
    return cleaned_acc_df


def process_all_data_sources(fb_present=True, miv_present=True, accident_present=True):
    """
    Process all data sources and turn them in to csv files. After this function is called there
    should be csv files of the cleaned and integrated data sources

    :param fb_present: bool, if the files present in local file system
    :param miv_present: bool, if the files present in local file system
    :param accident_present: bool, if the files present in local file system
    :return:
    """
    ensure_dirs_exist(data_dir, integrated_dir)
    logger.info("Started processing all data sources.")
    fb_to_integrated(fb_present)

    miv_to_integrated_csv(miv_present)

    acc_to_cleaned_geojson(accident_present)


def fb_to_integrated(files_present=True):

    start_time = time.time()
    logger.info("Start processing pedestrian and bicycle data (FootBikeCount)")
    fb_count_df = process_foot_bike_data(files_present)
    logger.debug(f'FB Head:{fb_count_df.head()}\n FB dtypes: {fb_count_df.dtypes}')
    fb_file_path = os.path.join(integrated_dir, 'FootBikeCount.csv')
    logger.debug(f'FB Cleaned File Path: {fb_file_path}')
    fb_count_df.to_csv(fb_file_path, index=False)
    logger.info("FB integrated csv created.")
    end_time = time.time()
    logger.info(f'Time taken for FootBikeCount: {end_time-start_time}')


def miv_to_integrated_csv(miv_present=True):

    start_time2 = time.time()
    logger.info("Start processing motorized vehicle data (MivCount)")
    miv_count_df = process_miv_data(miv_present)
    logger.debug(f'MIV Head:{miv_count_df.head()}\n MIV dtypes: {miv_count_df.dtypes}')
    miv_file_path = os.path.join(integrated_dir, 'MivCount.csv')
    logger.debug(f'MIV Cleaned File Path: {miv_file_path}')
    miv_count_df.to_csv(miv_file_path, index=False)
    logger.info("MIV integrated csv created.")
    end_time = time.time()
    logger.info(f'Time taken for MivCount: {end_time-start_time2}')


def acc_to_cleaned_geojson(acc_present=True):
    start_time3 = time.time()
    logger.info("Start processing accident data (Accidents)")
    acc_df = process_accident_data(acc_present)
    logger.debug(f'ACC Head: { acc_df.head()}\n Acc dtypes: {acc_df.dtypes}')
    acc_file_path = os.path.join(integrated_dir, 'Accidents.geojson')
    logger.debug(f'Acc Cleaned file path: {acc_file_path}')
    acc_df['geometry'] = acc_df['geometry'].apply(lambda row: re.findall(r"[-+]?\d*\.\d+|\d+", row))
    # Create a Point object using the extracted coordinates
    acc_df['geometry'] = acc_df['geometry'].apply(
        lambda coords: Point(float(coords[0]), float(coords[1]), float(coords[2])))
    acc_gdf = gpd.GeoDataFrame(acc_df, geometry='geometry')
    acc_gdf.to_file(acc_file_path, driver='GeoJSON')
    logger.info("ACC integrated csv created.")
    end_time = time.time()
    logger.info(f'Time taken for Accidents: {end_time - start_time3}')


if __name__ == '__main__':
    process_all_data_sources(True, True, True)
    # miv_to_integrated_csv()
    # acc_to_cleaned_geojson()
