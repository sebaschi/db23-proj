import os

import pandas as pd
import psycopg2
from psycopg2 import sql

integrated_dir = 'datasets/integrated/'

# Set up info needed to connect to db
db_info = {
    'host': '127.0.0.1',
    'database': 'zh-traffic',
    'port': '54322',
    'user': 'db23-db',
    'password': 'db23-project-role-PW@0',
    'sslmode': 'disable'
}

csv_table_maps = [
    {'file': os.path.join(integrated_dir, 'FootBikeCount.csv'), 'table': 'FootBikeCount'},
    {'file': os.path.join(integrated_dir, 'MivCount.csv'), 'table': 'MivCount'}
]

db_connection = psycopg2.connect(**db_info)


def csv_to_existing_table(csv_file_path, table_name):
    df = pd.read_csv(csv_file_path)
    curs = db_connection.cursor()
    df.to_sql(table_name, db_connection, if_exists='append', index_label=False)
    db_connection.commit()
    curs.close()


for i in csv_table_maps:
    csv_to_existing_table(i['file'], i['table'])

db_connection.close()
