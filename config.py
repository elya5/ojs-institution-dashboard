import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ARTICLE_CACHE_PATH = Path(os.getenv('CACHE_PATH') or 'cache')
BEACON_PATH = ARTICLE_CACHE_PATH / 'beacon.tab'
ROR_PATH = ARTICLE_CACHE_PATH / 'ror_data.csv'

DATAVERSE_API_KEY = os.getenv('DATAVERSE_API_KEY')
OPENALEX_API_KEY = os.getenv('OPENALEX_API_KEY')
ROR_API_KEY = os.getenv('ROR_API_KEY')

MIN_PUBLICATION_YEAR = 2021
