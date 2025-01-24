"""Module to export data from DB."""
import datetime
import logging
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, exc, inspect, text


def query_composition(query, config):
    """Compute the query and save the resolt as excel file.

    Args:
        query (str): First part of the sql query
        config (dict): Dictionary loaded from yml config file

    Returns:
        Sql Query
    """
    op_and = ''
    query = ''.join([query, ' where '])
    for field in config['query_filter'].keys():
        field_name = field.split(config['slit_character'])
        f_values = str(config['query_filter'][field]).split(config['slit_character'])
        if len(field_name) > 1 and len(f_values) > 1:
            temp_str = f"{field_name[0]}>='{f_values[0]}' and {field_name[1]}<='{f_values[1]}'"
        elif len(field_name) == 1 and len(f_values) > 1:
            temp_str = f"{field_name[0]}>='{f_values[0]}' and {field_name[0]}<='{f_values[1]}'"
        else:
            temp_str = f"{field_name[0]}='{f_values[0]}'"
        query = ''.join([query, op_and, temp_str])
        op_and = ' and '
    return query


def select_data(config, connection):
    """Compute the query and save the resolt as excel file.

    Args:
        config (dict): Dictionary loaded from yml config file
        connection (psycopg2.extensions.connection): Connection object

    """
    query = f"SELECT * FROM public.{config['db_access']['table']}"
    if (config['query_filter']):
        query = query_composition(query, config)
    logging.debug(f'QUERY: {query}')
    try:
        rs = connection.execute(text(query))
    except exc.ProgrammingError as syntax_error:
        logging.error(syntax_error)
        return
    logging.info('Query Executed')
    df = pd.DataFrame(rs.fetchall())
    if not df.empty:
        logging.info(f'Row Extracted {len(df)}')
        output_name_base = Path(__file__).parent.parent / 'system_folder'
        output_name_file = f"EXPORT_{config['db_access']['database']}_{config['db_access']['table']}_{datetime.datetime.now()}.xlsx"
        df.set_index('id', inplace=True)
        df.to_excel(output_name_base / output_name_file)
        logging.info('Excel file exported successfully')
        return
    logging.error('The query return 0 elements')


def db_connection(config):
    """Establish a connection to the database.

    Args:
        config (dict): Connection parameters

    Returns:
        psycopg2.extensions.connection: Connection object
    """
    db = create_engine(
        f'postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}'
    )
    try:
        connection = db.connect()
    except exc.OperationalError as err:
        logging.error(err)
        return None

    logging.info('Connection to DB done.')
    inspector = inspect(connection)

    table_exists = inspector.has_table(config['table'])
    if table_exists:
        return connection
    logging.error(f'Table "{config['table']}" not Found in DB, check the configuration file')
    return None


def export_initialization(conf_file):
    """Check the connection to DB.

    Args:
        conf_file (_type_): _description_
    """
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)  # noqa:WPS323
    logging.info(f'Configuration file Path: {conf_file}')

    if str(conf_file).endswith('yml'):

        with open(conf_file, 'r') as config_file:
            config_data = yaml.safe_load(config_file)
            conn = db_connection(config_data['db_access'])
            if conn:
                select_data(config_data, conn)
