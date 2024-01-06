import logging
import os
"""
The functionality of this script has been adapted from data_utils.ensure_dirs_exist().
This needs to be run before any other script.
"""
data_dir = 'datasets/'
integrated_dir = 'datasets/integrated/'
logs_dir = 'logs/'

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('integrate.py')
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.debug(f'data_dir: {data_dir}\n integrated_dir: {integrated_dir}')
logger.info("Ensuring needed directories exist.")
os.makedirs(data_dir, exist_ok=True)
logger.debug("data_dir created.")
os.makedirs(integrated_dir, exist_ok=True)
logger.debug("integrated_dir created")
os.makedirs(logs_dir, exist_ok=True)
logger.debug("logs_dir created")
