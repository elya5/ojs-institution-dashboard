import logging
from itertools import combinations

import country_converter as coco
import polars as pl

from api import article_disciplines
from config import BEACON_PATH
from data_loader import download_beacon_dataset

logger = logging.getLogger(__name__)


def get_journals() -> pl.LazyFrame:
    """Get a dataframe of all OJS journals in the beacon.csv"""
    logger.info('Reading OJS journals.')
    if not BEACON_PATH.exists():
        download_beacon_dataset()

    # TODO: filter out journals with less that X articles?
    return pl.scan_csv(BEACON_PATH)


def articles_to_publication_year_count(df: pl.LazyFrame) -> pl.DataFrame:
    return (
        df.group_by('publication_year')
        .agg(pl.col('id').len())
        .sort('publication_year')
        .collect()  # type: ignore
    )


def articles_to_disciplines_count(df: pl.LazyFrame, ror: str) -> pl.DataFrame:
    all_articles = (
        article_disciplines(ror)
        .rename({'key_display_name': 'Field'})
        .select('Field', 'count')
        .with_columns(pl.lit('All articles').alias('Type'))
    )
    df = (
        df.unnest('primary_topic', separator=':')
        .unnest('primary_topic:field', separator=':')
        .group_by('primary_topic:field:display_name')
        .agg(pl.col('id').len())
        .rename({'primary_topic:field:display_name': 'Field', 'id': 'count'})
        .sort('count', descending=True)
        .with_columns(pl.lit('In OJS Journal').alias('Type'))
        .select('Field', 'count', 'Type')
    )
    return pl.concat([df, all_articles]).collect()  # type: ignore


def articles_to_ojs_locations(df: pl.LazyFrame, mark_country_code: str) -> pl.DataFrame:
    journals = (
        get_journals()
        .select('issn', 'country_consolidated')
        .with_columns(pl.col('issn').str.split('\n').alias('issns'))
        .filter(pl.col('issns').is_not_null(), pl.col('issns').list.len() > 0)
    )

    return (
        df.unnest('primary_location', separator=':')
        .unnest('primary_location:source', separator=':')
        .explode('primary_location:source:issn')
        .join(
            journals.explode('issns'),
            left_on='primary_location:source:issn',
            right_on='issns',
        )
        .group_by('country_consolidated')
        .agg(pl.col('id').unique().len())
        .with_columns(
            pl.col('country_consolidated')
            .alias('country_name')
            .map_batches(
                lambda s: pl.Series(coco.convert(s, to='name_short')),
                return_dtype=pl.Utf8,
            ),
            pl.when(pl.col('country_consolidated') == mark_country_code)
            .then(pl.lit('Same country'))
            .otherwise(pl.lit('Different country'))
            .alias('color'),
        )
        .sort('id', descending=True)
        .collect()  # type: ignore
    )


def get_country_code_for_ror(articles: pl.LazyFrame, ror: str) -> str:
    df = articles.head(1).collect()
    return next(
        institution['country_code']
        for author in df['authorships'][0]  # type: ignore
        for institution in author['institutions']
        if institution['ror'] == ror
    )


def authors_to_country_collab_count(articles: pl.LazyFrame) -> pl.DataFrame:
    return (
        articles.explode('authorships')
        .unnest('authorships', separator=':')
        .unnest('authorships:author', separator=':')
        .filter(pl.col('authorships:institutions').list.len() > 0)
        .with_columns(
            pl.col('authorships:institutions').list.get(0).alias('institution')
        )
        .unnest('institution', separator=':')
        .rename({'institution:country_code': 'country_code'})
        .select('country_code', 'id')
        .group_by('id')
        .agg(pl.col('country_code').unique().drop_nulls())
        .filter(pl.col('country_code').list.len() >= 2)
        .with_columns(
            pl.col('country_code')
            .map_elements(
                lambda codes: list(combinations(codes, 2)),
                return_dtype=pl.List(pl.List(pl.String)),
            )
            .alias('pairs')
        )
        .explode('pairs')
        .group_by('pairs')
        .len()
        .sort('len', descending=True)
        .with_columns(
            pl.col('pairs').list.get(0).alias('a'),
            pl.col('pairs').list.get(1).alias('b'),
        )
        .rename({'len': 'count'})
        .with_columns(
            pl.col('a')
            .alias('country_a')
            .map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
            pl.col('b')
            .alias('country_b')
            .map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
        )
        .drop('a', 'b')
        .collect()  # type: ignore
    )
