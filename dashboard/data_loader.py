import logging
import shutil
import tempfile
import zipfile
from pathlib import Path

import pyalex
import requests
from config import (
    BEACON_PATH,
    DATAVERSE_API_KEY,
    OPENALEX_API_KEY,
    ROR_PATH,
)

logger = logging.getLogger(__name__)
DATAVERSE_URL = 'https://dataverse.harvard.edu/api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/OCZNVY'
pyalex.config.api_key = OPENALEX_API_KEY


def download_beacon_dataset() -> None:
    """Download beacon.csv from Harvard Dataverse."""
    logger.info('Downloading beacon.csv')
    response = requests.get(
        DATAVERSE_URL,
        headers={'X-Dataverse-key': DATAVERSE_API_KEY},
        stream=True,
        timeout=60,
    )

    BEACON_PATH.parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / 'dataset.zip'
        zip_path.write_bytes(response.content)
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(tmpdir_path)

        shutil.move(tmpdir_path / 'beacon.csv', BEACON_PATH)
    logger.info('Download completed')


def download_ror_dataset() -> None:
    """Download ROR csv dataset to ror_data.csv."""
    logger.info('Starting to download ROR entities.')
    r = requests.get('https://zenodo.org/api/records/18985120', timeout=10).json()
    r = requests.get(r['links']['files'], timeout=10).json()
    r = requests.get(r['entries'][0]['links']['content'], timeout=60)
    logger.info('Download complete. Starting to unpack and update database.')

    ROR_PATH.parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        tmppath = Path(tmp)
        zippath = tmppath / 'resp.zip'
        extractpath = tmppath / 'content'

        with zippath.open('wb') as f:
            f.write(r.content)

        with zipfile.ZipFile(zippath, 'r') as zip_ref:
            zip_ref.extractall(extractpath)

        csvpath = next(extractpath.glob('*csv'))
        shutil.move(csvpath, ROR_PATH)

    logger.info('Done.')
