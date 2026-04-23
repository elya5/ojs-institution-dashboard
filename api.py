import logging
from itertools import chain

import polars as pl
import pyalex
from pyalex import Works

from config import OPENALEX_API_KEY

logger = logging.getLogger(__name__)
pyalex.config.api_key = OPENALEX_API_KEY


def ojs_articles_by_year() -> pl.DataFrame:
    """Fetch all OJS articles grouped by year."""
    works = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}}, publication_year='>2020'
        )
        .group_by('publication_year')
        .get()
    )
    return pl.DataFrame(works).rename({'key': 'Year'}).drop('key_display_name')


def ojs_article_for_institution(ror: str, min_year: int = 2020) -> pl.DataFrame:
    """Fetch all OJS articles for the institution with pub year > min_year."""
    query = Works().filter(
        primary_location={'source': {'is_in_doaj': True}},
        publication_year='>' + str(min_year),
        authorships={'institutions': {'ror': ror}},
    )
    logger.info(f'Fetching {query.count()} articles for institution.')
    data = list(chain(*query.paginate(per_page=200)))
    return pl.DataFrame(data)


def share_of_ojs_articles(ror: str) -> pl.DataFrame:
    """Get share of OJS articles from all articles by the institution."""
    result = (
        Works()
        .filter(authorships={'institutions': {'ror': ror}})
        .group_by('primary_location.source.is_in_doaj')
        .get()
    )
    return pl.DataFrame(result)
