"""Program too check if there is new data to load in DB."""

import logging
import os
from pathlib import Path

import yaml
from load_data_service import getter_file_process


def check_process(cnf_file):
    """Check if there is new file to load.

    Args:
        cnf_file: Name of the configuration files.

    Returns:
        True or False if there is new files or not
    """
    with open(cnf_file, 'r') as conf_file:
        config_file = yaml.safe_load(conf_file)

    loading_data_path = Path(__file__).parent.parent / 'system_folder' / config_file['loading_data_folder_path']
    files_to_load = os.listdir(loading_data_path)

    if not files_to_load:
        logging.warning(f'{loading_data_path} Empty directory')
        return False
    not_allowed = []
    for data_file in files_to_load:
        if data_file.lower().endswith(('xls', 'xlsx')):
            logging.info('Found new data, starting loading service...')
            return True
        else:
            not_allowed.append(data_file)
    if not_allowed:
        logging.warning(f'Some data files are not allowed: {not_allowed}')

    logging.warning('No File Allowed Found')
    return False


def initialization(conf_file):
    """Start the Initialization process.

    Args:
        conf_file: Name of the configuration files.

    """
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)  # noqa:WPS323
    config_base_path = Path(__file__).parent / 'config'

    if str(conf_file).endswith('yml'):
        if check_process(config_base_path / conf_file):
            getter_file_process(conf_file)
    else:
        logging.warning(f'Config Files Not Allowed: {conf_file}')
