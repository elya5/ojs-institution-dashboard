import hashlib
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
    ROR_PATH,
)
from data_loader import get_journals

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


def get_openalex_countries() -> list[dict[str, str]]:
    """Get all available countries in OpenAlex."""
    countries = Works().group_by('authorships.institutions.country_code').get()
    return sorted(
        [
            {'key': country['key'].split('/')[-1], 'name': country['key_display_name']}
            for country in countries
        ],
        key=lambda c: c['name'],
    )


def ojs_articles_for_journal(issns: list[str]) -> pl.LazyFrame:
    query = (
        Works()
        .select(
            [
                'id',
                'fwci',
                'publication_year',
                'authorships',
                'primary_location',
                'primary_topic',
            ]
        )
        .filter(
            primary_location={'source': {'issn': '|'.join(issns)}},
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
        )
    )
    return get_openalex_articles(query)


def ojs_article_for_institution(ror: str) -> pl.LazyFrame:
    """Fetch all OJS articles for the institution with pub year > min_year."""
    query = (
        Works()
        .select(
            [
                'id',
                'fwci',
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
    return get_openalex_articles(query)


def get_openalex_articles(query: Works) -> pl.LazyFrame:
    """Abstract method to fetch and cache articles based on input query."""
    print(query.url)
    ARTICLE_CACHE_PATH.mkdir(exist_ok=True)
    cache_key = hashlib.sha256(query.url.encode()).hexdigest()
    cache_parquet = ARTICLE_CACHE_PATH / (cache_key + '.parquet')
    if cache_parquet.exists():
        return pl.scan_parquet(cache_parquet)

    num_articles = query.count()
    if num_articles > 20_000:
        raise ValueError(f'Too many articles (counted {num_articles})')
    logger.info(f'Fetching {num_articles} articles.')

    cache_jsonl = ARTICLE_CACHE_PATH / (cache_key + '.jsonl')
    cache_jsonl.unlink(missing_ok=True)
    for page in query.paginate(per_page=200):
        for article in page:
            with cache_jsonl.open('a') as f:
                f.write(json.dumps(article) + '\n')

    logger.info('Converting.')
    (
        pl.scan_ndjson(cache_jsonl)
        .rename({'id': 'work_id'})
        .explode('authorships')
        .unnest('authorships')
        .unnest('author')
        .rename({'id': 'author_id', 'display_name': 'author_name'})
        .filter(pl.col('institutions').list.len() > 0)
        .with_columns(pl.col('institutions').list.get(0).alias('institution'))
        .unnest('institution', separator=':')
        .unnest('primary_location')
        .unnest('source', separator=':')
        .unnest('primary_topic', separator=':')
        .unnest('primary_topic:field', separator=':')
        .rename(
            {
                'institution:ror': 'ror',
                'source:issn': 'issn',
                'primary_topic:field:display_name': 'field',
                'institution:country_code': 'country_code',
                'institution:display_name': 'institution_name',
            }
        )
        .filter(
            pl.col('ror').is_not_null(),
            pl.col('author_id').is_not_null(),
        )
        .with_columns(
            (1 / pl.col('work_id').count().over('work_id')).alias('author_weight')
        )
        .select(
            'fwci',
            'ror',
            'author_id',
            'author_name',
            'work_id',
            'publication_year',
            'issn',
            'field',
            'country_code',
            'institution_name',
            'author_weight',
        )
        .sink_parquet(cache_parquet)
    )
    cache_jsonl.unlink()

    return pl.scan_parquet(cache_parquet)


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
    return (
        pl.DataFrame(result)
        .with_columns(
            pl.when(pl.col('key_display_name') == 'true')
            .then(pl.lit('OJS'))
            .otherwise(pl.lit('Not OJS'))
            .alias('Publisher Software')
        )
        .rename({'count': 'Count'})
    )


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
    logger.info('Getting ROR institutions.')
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


def get_tops_table_country(country_code: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    query = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
            authorships={'countries': country_code},
        )
        .group_by('authorships.author.id')
    )
    result = query.get()
    authors = (
        pl.DataFrame(result)
        .select('key_display_name', 'count')
        .rename({'key_display_name': 'Author', 'count': 'Publications'})
        .limit(10)
    )

    query = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
            authorships={'countries': country_code},
        )
        .group_by('authorships.institutions.id')
    )
    result = query.get()
    institutions = (
        pl.DataFrame(result)
        .select('key_display_name', 'count')
        .rename({'key_display_name': 'Institution', 'count': 'Publications'})
        .limit(10)
    )
    return authors, institutions


def share_of_ojs_articles_country(country_code: str) -> pl.DataFrame:
    query = (
        Works()
        .filter(
            publication_year=f'>{MIN_PUBLICATION_YEAR - 1}',
            authorships={'countries': country_code},
        )
        .group_by('primary_location.source.is_in_doaj')
    )
    result = query.get()
    return (
        pl.DataFrame(result)
        .with_columns(
            pl.when(pl.col('key_display_name') == 'true')
            .then(pl.lit('OJS'))
            .otherwise(pl.lit('Not OJS'))
            .alias('Publisher Software')
        )
        .rename({'count': 'Count'})
    )


def publications_per_year_country(country_code: str) -> pl.DataFrame:
    works = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year='>2020',
            authorships={'countries': country_code},
        )
        .group_by('publication_year')
        .get()
    )
    return (
        pl.DataFrame(works)
        .rename({'key': 'Publication Year', 'count': 'Count'})
        .drop('key_display_name')
        .with_columns(pl.lit('Number of Works').alias('Dimension'))
    )


def publication_per_discipline_country(country_code: str) -> pl.DataFrame:
    works = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year='>2020',
            authorships={'countries': country_code},
        )
        .group_by('primary_topic.field.id')
        .get()
    )
    return (
        pl.DataFrame(works)
        .rename({'key_display_name': 'Field', 'count': 'Count'})
        .drop('key')
        .with_columns(pl.lit('In OJS Journal').alias('Type'))
    )


def publications_per_institutions_locations(country_code: str) -> pl.DataFrame:
    result = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year='>2020',
            authorships={'countries': country_code},
        )
        .group_by('authorships.institutions.ror')
        .get()
    )

    ror_df = (
        pl.read_csv(ROR_PATH)
        .select(
            'id',
            'locations.geonames_details.lat',
            'locations.geonames_details.lng',
            'names.types.ror_display',
        )
        .rename(
            {
                'locations.geonames_details.lat': 'lat',
                'locations.geonames_details.lng': 'lng',
                'names.types.ror_display': 'name',
            }
        )
    )

    return (
        pl.DataFrame(result)
        .join(ror_df, left_on='key', right_on='id')
        .select('count', 'name', 'lat', 'lng')
        .rename({'name': 'Institution', 'count': 'weight'})
    )


def publication_to_journal_location_country(country_code: str) -> pl.DataFrame:
    result = (
        Works()
        .filter(
            primary_location={'source': {'is_in_doaj': True}},
            publication_year='>2020',
            authorships={'countries': country_code},
        )
        .group_by('primary_location.source.issn')
        .get()
    )

    journals = (
        get_journals()
        .select('issn', 'country_consolidated')
        .with_columns(pl.col('issn').str.split('\n').alias('issns'))
        .filter(pl.col('issns').is_not_null(), pl.col('issns').list.len() > 0)
        .explode('issns')
        .collect()
    )

    return (
        pl.DataFrame(result)
        .join(journals, left_on='key', right_on='issns')  # type: ignore
        .group_by('issn')
        .agg(
            pl.col('count').first(),
            pl.col('country_consolidated').first().str.to_uppercase(),
        )
        .group_by('country_consolidated')
        .agg(pl.col('count').sum())
        .rename({'country_consolidated': 'country_code'})
    )
