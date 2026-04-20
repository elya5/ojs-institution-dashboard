import logging
import os
import shutil
import tempfile
import zipfile
from itertools import chain
from pathlib import Path

import click
import pyalex
import requests
from dotenv import load_dotenv
from pyalex import Works

load_dotenv()
logger = logging.getLogger(__name__)
DATAVERSE_API_KEY = os.getenv("DATAVERSE_API_KEY")
DATAVERSE_URL = "https://dataverse.harvard.edu/api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/OCZNVY"
pyalex.config.api_key = os.getenv("OPENALEX_API_KEY")


@click.command()
def download_beacon_dataset() -> Path:
    """Download beacon.csv from Harvard Dataverse."""
    logger.info('Downloading beacon.csv')
    response = requests.get(
        DATAVERSE_URL,
        headers={"X-Dataverse-key": DATAVERSE_API_KEY},
        stream=True,
        timeout=60
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / "dataset.zip"
        zip_path.write_bytes(response.content)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmpdir_path)

        shutil.move(tmpdir_path / 'beacon.csv', Path('beacon.csv'))
    logger.info('Download completed')
    return Path('beacon.csv')


def download_openalex_journal_articles(issns: list[str]) -> list[dict]:
    query = Works().filter(primary_location={'source': {'issn': '|'.join(issns)}})
    return list(chain(*query.paginate(per_page=200)))


@click.command()
def download_ror_dataset() -> Path:
    """Download ROR csv dataset to ror_data.csv."""
    logger.info('Starting to download ROR entities.')
    r = requests.get("https://zenodo.org/api/records/18985120", timeout=10).json()
    r = requests.get(r['links']['files'], timeout=10).json()
    r = requests.get(r['entries'][0]['links']['content'], timeout=60)
    logger.info('Download complete. Starting to unpack and update database.')

    target_location = Path('ror_data.csv')
    with tempfile.TemporaryDirectory() as tmp:
        tmppath = Path(tmp)
        zippath = tmppath / 'resp.zip'
        extractpath = tmppath / 'content'

        with zippath.open('wb') as f:
            f.write(r.content)

        with zipfile.ZipFile(zippath, 'r') as zip_ref:
            zip_ref.extractall(extractpath)

        csvpath = next(extractpath.glob('*csv'))
        shutil.move(csvpath, target_location)

    logger.info('Done.')
    return target_location
