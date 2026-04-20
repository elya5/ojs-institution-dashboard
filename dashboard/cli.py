import logging

import click
from data_loader import download_beacon_dataset, download_ror_dataset

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)


@click.group()
def cli():
    pass


cli.add_command(download_beacon_dataset)
cli.add_command(download_ror_dataset)


if __name__ == '__main__':
    cli()
