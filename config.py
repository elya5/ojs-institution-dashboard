from pathlib import Path

import streamlit as st

BEACON_PATH = Path('data/beacon.csv')
ROR_PATH = Path('data/ror_data.csv')
ARTICLE_CACHE_PATH = Path('cache.jsonl')

DATAVERSE_API_KEY = st.secrets['DATAVERSE_API_KEY']
OPENALEX_API_KEY = st.secrets['OPENALEX_API_KEY']

MIN_PUBLICATION_YEAR = 2020
