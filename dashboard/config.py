import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BEACON_PATH = Path('data/beacon.csv')
ROR_PATH = Path('data/ror_data.csv')

DATAVERSE_API_KEY = os.getenv('DATAVERSE_API_KEY')
OPENALEX_API_KEY = os.getenv('OPENALEX_API_KEY')
