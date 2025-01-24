"""Service to load new data acording to config file."""
import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, exc, inspect


def db_connection(config):
    """Establish a connection to the database.

    Args:
        config (dict): Connection parameters

    Returns:
        psycopg2.extensions.connection: Connection object
    """
    db = create_engine(
        f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )

    try:
        connection = db.connect()
    except exc.OperationalError as err:
        logging.error(err)
        return 0

    logging.info('Connection established')
    inspector = inspect(connection)
    table_exists = inspector.has_table(config['table'])
    if table_exists:
        return connection
    logging.error(f'Table "{config['table']}" not Found in DB, check the configuration file')
    return 0


def load_data(data_path, connection, config):
    """Read data and load in db.

    Args:
        data_path (Path): path to the file
        connection (psycopg2.extensions.connection): Connection object
        config (Dict): Configuration information

    Returns:
        The outcome of the process

    """

    df = pd.read_excel(data_path, index_col='id')
    loading = df.to_sql(config['table'], schema='public', con=connection, if_exists='replace', index=True)
    connection.commit()
    return loading


def getter_file_process(config_file):
    """Check if there is new file to load.

    Args:
        config_file: Name of the configuration files.

    """
    with open(config_file, 'r') as conf_file:
        config_file = yaml.safe_load(conf_file)

    loading_folder = Path(__file__).parent.parent / 'system_folder' / config_file['loading_data_folder_path']
    complete_folder = Path(__file__).parent.parent / 'system_folder' / config_file['complete_data_folder_path']
    connection = db_connection(config_file['db_access'])
    if connection:
        for data_file in os.listdir(loading_folder):
            if data_file.lower().endswith(('xls', 'xlsx')):
                data_path = Path(loading_folder) / data_file
                new_path = Path(complete_folder) / data_file
                loading = load_data(data_path, connection, config_file['db_access'])
                if loading:
                    logging.info(f'Data from "{data_path}" saves successfully\n')
                    os.rename(data_path, new_path)
                    logging.info(f'File "{data_path}" moved to "{new_path}"')
                else:
                    logging.error(f'Loading failed, file: {data_path}')
