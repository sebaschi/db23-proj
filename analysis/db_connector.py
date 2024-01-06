import logging

import paramiko.util
import sqlalchemy
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import SSH_HOST, SSH_USERNAME, SSH_PASSWORD, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, SSH_PORT

logging.getLogger("paramiko").setLevel(logging.WARNING)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('db_connector.py')
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class RemoteDB:
    def __init__(self):
        self.ssh_host = SSH_HOST
        self.ssh_username = SSH_USERNAME
        self.ssh_password = SSH_PASSWORD
        self.db_name = DB_NAME
        self.db_user = DB_USER
        self.db_password = DB_PASSWORD
        self.db_host = DB_HOST
        self.db_port = DB_PORT
        self.ssh_port = SSH_PORT
        self.tunnel = None
        self.engine = None
        self.Session = None
        self._connect()

    def _connect(self):
        try:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_password=self.ssh_password,
                remote_bind_address=(self.db_host, self.db_port)
            )
            self.tunnel.start()

            local_port = self.tunnel.local_bind_port
            db_url = f"postgresql://{self.db_user}:{self.db_password}@localhost:{local_port}/{self.db_name}"
            self.engine = create_engine(db_url)
            self.Session = sessionmaker(bind=self.engine)
        except Exception as e:
            logger.exception(f"Connection failed: {e}")

    def execute_query(self, query: str):
        session = self.Session()
        try:
            result = session.execute(sqlalchemy.text(query))
            session.commit()
            return result.fetchall()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_command(self, cmd: str):
        session = self.Session()
        try:
            result = session.execute(sqlalchemy.text(cmd))
            session.commit()
            logger.debug(f"Command {cmd} committed.")
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self):
        if self.engine:
            self.engine.dispose()
        if self.tunnel:
            self.tunnel.stop()
