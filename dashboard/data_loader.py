import logging
import os
import shutil
import tempfile
import zipfile
from itertools import chain
from pathlib import Path

import pyalex
import requests
from dotenv import load_dotenv
from pyalex import Works


load_dotenv()
logger = logging.getLogger(__file__)
DATAVERSE_API_KEY = os.getenv("DATAVERSE_API_KEY")
DATAVERSE_URL = "https://dataverse.harvard.edu/api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/OCZNVY"
pyalex.config.api_key = os.getenv("OPENALEX_API_KEY")


def download_beacon_dataset() -> Path:
    """Download beacon.csv from Harvard Dataverse."""
    logger.info('Downloading beacon.csv')
    response = requests.get(DATAVERSE_URL, headers={"X-Dataverse-key": DATAVERSE_API_KEY}, stream=True)

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
