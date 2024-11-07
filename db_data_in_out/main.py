#!/usr/bin/env python
"""Program to import/export data from/to excel files."""
import logging
from pathlib import Path

import typer
from check_service import initialization
from export_service import export_initialization

app = typer.Typer()


def check_input_file(input_file):
    """Check if the file exist.

    Args:
        input_file: Path for the configuration files.

    Returns:
        The path of the file or None
    """
    config_base_path = Path().absolute().parent / 'config'
    conf_file = config_base_path / input_file

    if Path(input_file).exists():
        return input_file
    elif Path(conf_file).exists():
        return conf_file


@app.command()
def check_new_file(input_file: str = typer.Option('load_config.yml', help='Path for the configuration files.')) -> bool:
    """Check if there is new file to load.

    Args:
        input_file: Path for the configuration files.

    Returns:
        True or False if there is new files or not
    """
    config_file = check_input_file(input_file)
    if config_file:
        return initialization(config_file)
    else:
        logging.error('File not found: {0}'.format(input_file))


@app.command()
def export(input_file: str = typer.Option('export_config.yml', help='Path for the configuration files.')) -> bool:
    """Export service.

    Args:
        input_file: Path for the configuration files.

    Returns:
        True or False if there is new files or not
    """
    config_file = check_input_file(input_file)
    if config_file:
        return export_initialization(config_file)
    else:
        logging.error('File not found: {0}'.format(input_file))


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)  # noqa:WPS323
    app()
