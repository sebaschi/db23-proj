from db_connector import RemoteDB
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("db_utils.py")
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# Generic DB Methods ==================================================================================================
def drop_view(view_name):
    drop_view_sql = f"""
    DROP VIEW IF EXISTS {view_name};
    """

    remote_db = RemoteDB()
    try:
        result = remote_db.execute_command(drop_view_sql)
        logger.info(f"{view_name} dropped.")
    except Exception as e:
        logger.exception(f"Exception while dropping {view_name}. Msg: {e} ")
    finally:
        remote_db.close()
        logger.debug(f"RemoteDB object closed.")


def get_view(view_name, rows="*"):
    get_view_sql = f"""
    SELECT {rows}
    FROM {view_name};
    """
    remote_db = RemoteDB()
    try:
        result = remote_db.execute_query(get_view_sql)
        logger.info(f"Succesfully retrieved {view_name}")
        return result
    except Exception as e:
        logger.exception(f"Failed getting view for {view_name} with exception {e}.")
    finally:
        remote_db.close()

def query_table(table_name):
    pass


# Specialized DB methods ==============================================================================================
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
    logger.info("Heat View Created")


def create_bike_heat_view():
    create_heat_view_sql = """
        CREATE VIEW bikeheat AS
    SELECT
        ST_Y(geometry) AS latitude,
        ST_X(geometry) AS longitude,
        AccidentYear AS year
    FROM
        accidents
    WHERE
        ST_Y(geometry) IS NOT NULL AND
        ST_X(geometry) IS NOT NULL AND
        AccidentYear IS NOT NULL AND
        accidentinvolvingbicycle IS TRUE;
    """

    remote_db = RemoteDB()
    remote_db.execute_command(create_heat_view_sql)
    remote_db.close()
    logger.info("BIKE Heat View Created")


def create_pedestrian_heat_view():
    create_heat_view_sql = """
        CREATE VIEW pedestrianheat AS
    SELECT
        ST_Y(geometry) AS latitude,
        ST_X(geometry) AS longitude,
        AccidentYear AS year
    FROM
        accidents
    WHERE
        ST_Y(geometry) IS NOT NULL AND
        ST_X(geometry) IS NOT NULL AND
        AccidentYear IS NOT NULL AND
        accidentinvolvingpedestrian IS TRUE;
    """

    remote_db = RemoteDB()
    remote_db.execute_command(create_heat_view_sql)
    remote_db.close()
    logger.info("PEDESTRIAN Heat View Created")


def create_motorcycle_heat_view():
    create_heat_view_sql = """
        CREATE VIEW motoheat AS
    SELECT
        ST_Y(geometry) AS latitude,
        ST_X(geometry) AS longitude,
        AccidentYear AS year
    FROM
        accidents
    WHERE
        ST_Y(geometry) IS NOT NULL AND
        ST_X(geometry) IS NOT NULL AND
        AccidentYear IS NOT NULL AND
        accidentinvolvingpedestrian IS TRUE;
    """

    remote_db = RemoteDB()
    remote_db.execute_command(create_heat_view_sql)
    remote_db.close()
    logger.info("MOTO Heat View Created")