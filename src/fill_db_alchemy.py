import os
import pandas as pd
from sqlalchemy import create_engine

integrated_dir = 'datasets/integrated/'

# Set up info needed to connect to db
db_info = {
    'host': 'localhost',
    'database': 'test-db23',
    'port': '5432',
    'user': 'seb',
    'password': '',
}

csv_table_maps = [
    {'file': os.path.join(integrated_dir, 'FootBikeCount.csv'), 'table': 'FootBikeCount'},
    {'file': os.path.join(integrated_dir, 'MivCount.csv'), 'table': 'MivCount'}
]

# Create a SQLAlchemy engine
engine = create_engine(
    f"postgresql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}",
    echo=True  # Set echo to True to display SQL queries (optional)
)

def csv_to_existing_table(csv_file_path, table_name):
    df = pd.read_csv(csv_file_path)
    df.to_sql(table_name, engine, if_exists='append', index=False)

for i in csv_table_maps:
    csv_to_existing_table(i['file'], i['table'])

# Close the SQLAlchemy engine
engine.dispose()
