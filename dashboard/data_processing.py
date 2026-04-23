import logging

import polars as pl

logger = logging.getLogger(__name__)


def get_journals() -> pl.DataFrame:
    """Get a dataframe of all OJS journals in the beacon.csv"""
    logger.info('Reading OJS journals.')
    # TODO: filter out journals with less that X articles?
    return pl.read_csv('data/beacon.csv')


def articles_to_publication_year_count(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by('publication_year').agg(pl.col('id').len()).sort('publication_year')
    )


def articles_to_disciplines_count(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.unnest('primary_topic', separator=':')
        .unnest('primary_topic:field', separator=':')
        .group_by('primary_topic:field:display_name')
        .agg(pl.col('id').len())
        .sort('id', descending=True)
    )
