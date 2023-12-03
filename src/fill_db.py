import logging
import psycopg2
import subprocess

logging.basicConfig(level=logging.DEBUG, filename='logs/fill_db.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fill_db.py')
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

integrated_dir = 'datasets/integrated/'
accident_geojson_file = 'datasets/integrated/Accidents.geojson'
accident_loader_script = 'load_accidents_into_db.sh'
accident_table_name = 'accidents'

db_info = {
    'host': 'localhost',
    'database': 'test-db23',
    'port': '5432',
    'user': 'seb',
    'password': '',
}
setup_tables_script = 'setup_tables.sql'
load_csvs_into_db_script = 'load_csvs_into_db.sql'



def run_sql(script, db_info):
    db_connection = psycopg2.connect(**db_info)
    db_cursor = db_connection.cursor()

    with open(script, 'r') as sql_file:
        sql_script = sql_file.read()

    try:
        db_cursor.execute(sql_script)
        db_connection.commit()
        logger.info(f'{script} executed successfully')
    except Exception as e:
        db_connection.rollback()
        logger.exception(f'Error executing {sql_script}: {e}')
    finally:
        db_cursor.close()
        db_connection.close()


def run_geojson_loader_script(script, *args):

    try:
        cmd = ['bash', script] + list(args)
        res = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f'{script} executed successfully. Output: {res.stdout}')
    except subprocess.CalledProcessError as e:
        logger.exception(f'Error executing {script}: {e}')
        logger.info(f"Remember to set the correct permissions for the script: chmod +x {script}")


def geojson_loader(*args):
    geojson_file, db_name, db_user, db_password, db_host, db_port, target_table = args
    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        f"PG:dbname='{db_name}' host='{db_host}' port='{db_port}' user='{db_user}' password='{db_password}'",
        geojson_file,
        "-nln", target_table,
        "-append"
    ]
    try:
        # Run the command
        res = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"ogr2ogr command executed successfully. Output: {res.stdout}")
    except subprocess.CalledProcessError as e:
        logger.exception(f"Error executing ogr2ogr command: {e}")


if __name__ == '__main__':
    run_sql(setup_tables_script, db_info)
    logger.info("Finnished setting up tables.")
    run_sql(load_csvs_into_db_script, db_info)
    logger.info("Finnished loading csv into db.")
    run_geojson_loader_script(accident_loader_script,
                              accident_geojson_file,
                              db_info['database'],
                              db_info['user'],
                              db_info['password'],
                              db_info['host'],
                              db_info['port'],
                              accident_table_name)
    logger.info('Finished loading geojson into db using bash script.')

