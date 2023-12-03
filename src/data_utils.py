import os
import pandas as pd
import requests
from urllib.parse import urlparse
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor as tpe
import logging

logging.basicConfig(level=logging.DEBUG, filename='data_utils.log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_utils.py')


def download_csv(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def process_urls(data_dir, urls_file):
    # Ensure the data directory exists
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Read URLs from the file
    with open(urls_file, 'r') as file:
        urls = file.readlines()

    # Process each URL
    for url in urls:
        url = url.strip()
        filename = os.path.basename(urlparse(url).path)
        local_filename = os.path.join(data_dir, filename)

        # Check if the file already exists
        if not os.path.isfile(local_filename):
            logger.debug(f"Downloading {url}...")
            download_csv(url, local_filename)
            logger.debug(f"Saved to {local_filename}")
        else:
            print(f"File {filename} already exists in {data_dir}, skipping download.")


def load_dataframe_from_csv(filepath):
    try:
        df = pd.read_csv(filepath, low_memory=False)
        return df
    except Exception as e:
        logger.error(f"Error loading {filepath}: {e}")
        return None


def load_dataframes_from_csv_files(data_dir, u_string):
    dataframes = []

    with tpe(max_workers=5) as executor:
        for filename in os.listdir(data_dir):
            if (u_string in filename) and filename.endswith('.csv'):
                filepath = os.path.join(data_dir, filename)
                future = executor.submit(load_dataframe_from_csv, filepath)
                dataframes.append(future)

    dataframes = [future.result() for future in dataframes if future.result() is not None]

    return dataframes

    # for filename in os.listdir(data_dir):
    #     if (u_string in filename) and filename.endswith('.csv'):
    #         filepath = os.path.join(data_dir, filename)
    #         df = pd.read_csv(filepath, low_memory=False)
    #         dataframes.append(df)
    # return dataframes


def load_dataframes_from_geojson_files(data_dir, u_string):
    print('u_string', u_string)
    gdf = gpd.GeoDataFrame()
    for filename in os.listdir(data_dir):
        #print("Filename:", filename)
        if (u_string in filename) and filename.endswith('.json'):
            filepath = os.path.join(data_dir, filename)
            print("Filepath:", filepath)
            gdf = gpd.read_file(filepath)

    return gdf


def combine_dataframes(dataframes):
    if dataframes:
        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        return combined_dataframe
    else:
        print("No dataframes to combine")
        return pd.DataFrame()


def create_unified_df(urls_file, u_string, data_dir, files_present=False):
    df_list = []
    df_unified = None
    if not files_present:
        process_urls(data_dir, urls_file)

    df_list = load_dataframes_from_csv_files(data_dir, u_string)
    df_unified = combine_dataframes(df_list)

    return df_unified


def save_dataframe_to_csv(df, integrated_dir, filename):
    pass


if __name__ == "__main__":
    csv_urls_file = '../docs/all_csv_urls.txt'
    datasets_dir = 'datasets/'
    output_file = 'column_names.txt'
    process_urls(datasets_dir, csv_urls_file)
    # extract_column_names(datasets_dir, output_file)
