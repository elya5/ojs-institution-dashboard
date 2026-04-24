import json
import logging

import polars as pl
import pyalex
import requests
from pyalex import Works

from config import (
    ARTICLE_CACHE_PATH,
    MIN_PUBLICATION_YEAR,
    OPENALEX_API_KEY,
    ROR_API_KEY,
)

logger = logging.getLogger(__name__)
pyalex.config.api_key = OPENALEX_API_KEY


def ojs_articles_by_year() -> pl.DataFrame:
    """Fetch all OJS articles grouped by year."""
    logger.info('Fetching all OJS articles per year.')
    works = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}}, publication_year='>2020'
        )
        .group_by('publication_year')
        .get()
    )
    return pl.DataFrame(works).rename({'key': 'Year'}).drop('key_display_name')


def ojs_article_for_institution(ror: str) -> pl.LazyFrame:
    """Fetch all OJS articles for the institution with pub year > min_year."""

    query = (
        Works()
        .select(
            [
                'id',
                'publication_year',
                'authorships',
                'primary_location',
                'primary_topic',
            ]
        )
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
            authorships={'institutions': {'ror': ror}},
        )
    )
    num_articles = query.count()
    if num_articles > 10_000:
        raise ValueError(f'Too many articles ({num_articles})')
    logger.info(f'Fetching {num_articles} articles for institution.')

    ARTICLE_CACHE_PATH.unlink(missing_ok=True)
    for page in query.paginate(per_page=200):
        for article in page:
            with ARTICLE_CACHE_PATH.open('a') as f:
                f.write(json.dumps(article) + '\n')

    return pl.scan_ndjson(ARTICLE_CACHE_PATH)


def share_of_ojs_articles(ror: str) -> pl.DataFrame:
    """Get share of OJS articles from all articles by the institution."""
    logger.info('Fetching share of OJS article to total articles by institution.')
    result = (
        Works()
        .filter(
            authorships={'institutions': {'ror': ror}},
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
        )
        .group_by('primary_location.source.is_in_doaj')
        .get()
    )
    return pl.DataFrame(result)


def article_disciplines(ror: str) -> pl.LazyFrame:
    logger.info('Fetching distribution of disciplines for all articles by institution.')
    return pl.LazyFrame(
        Works()
        .filter(
            authorships={'institutions': {'ror': ror}},
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
        )
        .group_by('primary_topic.field.id')
        .get(),
        schema_overrides={'count': pl.UInt32},
    )


def get_ror_suggestions(text: str) -> list[dict]:
    """Retrieve 50 best matches for ROR ID + name for a given input text."""
    r = requests.get(
        'https://api.ror.org/organizations?query=' + text,
        headers={'Client-Id': ROR_API_KEY},
        timeout=5,
    )
    raw_options = r.json()['items'][:50]
    return [
        {
            'ror': o['id'],
            'name': next(n['value'] for n in o['names'] if 'ror_display' in n['types']),
        }
        for o in raw_options
    ]
