import data_utils as du
from datetime import datetime as dt
import os
import requests
import pandas as pd

foot_bike_urls_file = '../docs/foot_bike_zaehlung_urls.txt'
miv_file_urls = '../docs/verkehrszaehlung_moto_urls.txt'
accident_file_url = '../docs/accident_loc_urls.txt'

# Using u_string to discriminate between files that belong to each other
motor_file_u_string = 'sid_dav_verkehrszaehlung_miv_OD2031'
foot_bike_file_u_string = 'velo.csv'
accident_file_u_string = 'RoadTrafficAccidentLocations.json'

data_dir = 'datasets/'
integrated_dir = 'datasets/integrated/'

weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def process_foot_bike_data():
    fb_df_unified = du.create_unified_df(foot_bike_urls_file, foot_bike_file_u_string, data_dir, files_present=True)
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
    return cleaned_fb_df


def process_miv_data():
    miv_df_unified = du.create_unified_df(miv_file_urls, motor_file_u_string, data_dir,files_present=True)

    miv_df_unified[['Date', "Time"]] = miv_df_unified['MessungDatZeit'].str.split('T', expand=True)
    miv_df_unified[['Hrs', 'Mins', 'Sec']] = miv_df_unified['Time'].str.split(':', expand=True)

    miv_cols_to_keep = ['MSID','ZSID','Achse', 'EKoord', 'NKoord', 'Richtung', 'AnzFahrzeuge', 'AnzFahrzeugeStatus',
                        'Date', 'Hrs']
    miv_df_cols_dropped = miv_df_unified[miv_cols_to_keep]

    dt_obj = pd.to_datetime(miv_df_cols_dropped['Date'])
    days = dt_obj.dt.weekday
    miv_df_cols_dropped['Weekday_en'] = days.map(lambda x: weekday_names[x])


    cleaned_miv_df = miv_df_cols_dropped
    return cleaned_miv_df


def process_accident_data():

    acc_df_unified = du.load_dataframes_from_geojson_files(data_dir, accident_file_u_string)
    acc_cols_to_keep = ['AccidentUID', 'AccidentHour', 'AccidentYear', 'AccidentWeekDay_en', 'AccidentType',
                        'AccidentSeverityCategory', 'AccidentInvolvingPedestrian', 'AccidentInvolvingBicycle',
                        'AccidentInvolvingMotorcycle', 'RoadType', 'RoadType_en', 'AccidentLocation_CHLV95_E',
                        'AccidentLocation_CHLV95_N', 'geometry']
    cleaned_acc_df = acc_df_unified[acc_cols_to_keep]
    return cleaned_acc_df


if __name__ == '__main__':
    fb_df = process_miv_data()
    print(fb_df['MessungDatZeit'])
    print(fb_df.dtypes)
    print(fb_df.head(100))
