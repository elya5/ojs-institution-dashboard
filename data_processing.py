import logging

import country_converter as coco
import polars as pl

from api import article_disciplines
from config import BEACON_PATH, ROR_PATH
from data_loader import download_beacon_dataset, download_ror_dataset

logger = logging.getLogger(__name__)


def get_journals() -> pl.LazyFrame:
    """Get a dataframe of all OJS journals in the beacon.csv"""
    logger.info('Reading OJS journals.')
    if not BEACON_PATH.exists():
        download_beacon_dataset()

    # TODO: filter out journals with less that X articles?
    return pl.scan_csv(BEACON_PATH)


def articles_to_publication_year_count(
    df: pl.LazyFrame, fractional: bool, ror: str | None = None
) -> pl.DataFrame:
    if fractional and ror is not None:
        return (
            df.filter(pl.col('ror') == ror)
            .group_by('publication_year')
            .agg(pl.col('author_weight').sum())
            .sort('publication_year')
            .rename({'publication_year': 'Publication Year', 'author_weight': 'Count'})
            .collect()  # type: ignore
        )
    elif fractional and ror is None:
        raise Exception('Invalid function arguments')
    else:
        return (
            df.group_by('publication_year')
            .agg(pl.col('work_id').unique().len())
            .sort('publication_year')
            .rename({'publication_year': 'Publication Year', 'work_id': 'Count'})
            .collect()  # type: ignore
        )


def articles_to_disciplines_count(
    df: pl.LazyFrame, ror: str, fractional: bool
) -> pl.DataFrame:
    if fractional:
        return (
            df.filter(pl.col('ror') == ror)
            .group_by('field')
            .agg(pl.col('author_weight').sum().alias('Count'))
            .rename({'field': 'Field'})
            .sort('Count', descending=True)
            .with_columns(pl.lit('In OJS Journal').alias('Type'))
            .select('Field', 'Count', 'Type')
            .collect()  # type: ignore
        )
    else:
        all_articles = (
            article_disciplines(ror)
            .rename({'key_display_name': 'Field'})
            .select('Field', 'count')
            .rename({'count': 'Count'})
            .with_columns(pl.lit('All articles').alias('Type'))
        )
        df = (
            df.group_by('field')
            .agg(pl.col('work_id').unique().len())
            .rename({'field': 'Field', 'work_id': 'Count'})
            .sort('Count', descending=True)
            .with_columns(pl.lit('In OJS Journal').alias('Type'))
            .select('Field', 'Count', 'Type')
        )
        return pl.concat([df, all_articles]).collect()  # type: ignore


def articles_to_ojs_locations(
    df: pl.LazyFrame, fractional: bool, ror: str
) -> pl.DataFrame:
    mark_country_code = get_country_code_for_ror(df, ror)
    journals = (
        get_journals()
        .select('issn', 'country_consolidated')
        .with_columns(pl.col('issn').str.split('\n').alias('issns'))
        .filter(pl.col('issns').is_not_null(), pl.col('issns').list.len() > 0)
    )

    df = (
        df.filter(pl.col('ror') == ror)
        .explode('issn')
        .join(journals.explode('issns'), left_on='issn', right_on='issns')
    )

    if fractional and ror is not None:
        df = df.group_by('country_consolidated').agg(
            pl.col('author_weight').sum().alias('Count')
        )
    elif fractional and ror is None:
        raise Exception('Invalid function arguments')
    else:
        df = df.group_by('country_consolidated').agg(
            pl.col('work_id').unique().len().alias('Count')
        )

    return (
        df.with_columns(
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
        .rename({'country_name': 'Country Name'})
        .sort('Count', descending=True)
        .collect()
    )


def get_country_code_for_ror(articles: pl.LazyFrame, ror: str) -> str:
    return articles.filter(pl.col('ror') == ror).head(1).collect()['country_code'][0]  # type:ignore


def articles_to_country_collab_count(
    articles: pl.LazyFrame, fractional: bool
) -> dict[str, pl.DataFrame]:
    if fractional:
        # TODO: how to measure edge for fractional counting
        nodes = articles.group_by('country_code').agg(
            pl.col('author_weight').sum().alias('weight')
        )
        df = articles.group_by(['work_id', 'country_code']).agg(
            pl.col('author_weight').sum().alias('country_weight')
        )
        df = (
            df.join(df, on='work_id', how='inner', suffix='_b')
            .filter(pl.col('country_code') < pl.col('country_code_b'))
            .with_columns(
                (pl.col('country_weight') + pl.col('country_weight_b')).alias(
                    'pair_weight'
                )
            )
        )
    else:
        nodes = articles.group_by('country_code').len().rename({'len': 'weight'})
        df = articles.select('work_id', 'country_code').unique()
        df = (
            df.join(df, on='work_id', how='inner', suffix='_b')
            .filter(pl.col('country_code') < pl.col('country_code_b'))
            .with_columns(pl.lit(1).alias('pair_weight'))
        )

    nodes = (
        nodes.with_columns(
            pl.col('country_code')
            .alias('country')
            .map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
        )
        .filter(pl.col('country') != 'not found')
        .select('weight', 'country')
        .collect()
    )
    edges = (
        df.group_by(['country_code', 'country_code_b'])
        .agg(pl.col('pair_weight').sum().alias('count'))
        .sort('count', descending=True)
        .with_columns(
            pl.col('country_code')
            .alias('country_a')
            .map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
            pl.col('country_code_b')
            .alias('country_b')
            .map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
        )
        .select('country_a', 'country_b', 'count')
        .collect()
    )
    return {'edges': edges, 'nodes': nodes}  # type: ignore


def articles_to_institution_collab_count(
    articles: pl.LazyFrame, fractional: bool
) -> dict[str, pl.DataFrame]:
    if not ROR_PATH.exists():
        download_ror_dataset()

    ror_df = (
        pl.scan_csv(ROR_PATH)
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

    if fractional:
        nodes = articles.group_by('ror').agg(
            pl.col('author_weight').sum().alias('weight')
        )
        df = articles.group_by(['work_id', 'ror']).agg(
            pl.col('author_weight').sum().alias('ror_weight')
        )
        df = (
            df.join(df, on='work_id', how='inner', suffix='_b')
            .filter(pl.col('ror') < pl.col('ror_b'))
            .with_columns(
                (pl.col('ror_weight') + pl.col('ror_weight_b')).alias('pair_weight')
            )
        )
    else:
        nodes = articles.group_by('ror').len().rename({'len': 'weight'})
        df = articles.select('work_id', 'ror').unique()
        df = (
            df.join(df, on='work_id', how='inner', suffix='_b')
            .filter(pl.col('ror') < pl.col('ror_b'))
            .with_columns(pl.lit(1).alias('pair_weight'))
        )

    nodes = (
        nodes.join(ror_df, left_on='ror', right_on='id')
        .select('weight', 'name', 'lat', 'lng')
        .rename({'name': 'Institution'})
        .collect()
    )
    edges = (
        df.group_by(['ror', 'ror_b'])
        .agg(pl.col('pair_weight').sum().alias('count'))
        .join(ror_df, left_on='ror', right_on='id')
        .rename({'name': 'Institution 1', 'lat': 'lat1', 'lng': 'lng1'})
        .join(ror_df, left_on='ror_b', right_on='id')
        .rename({'name': 'Institution 2', 'lat': 'lat2', 'lng': 'lng2'})
        .select(
            'Institution 1', 'lat1', 'lng1', 'Institution 2', 'lat2', 'lng2', 'count'
        )
        .sort('count', descending=True)
        .collect()
    )
    return {'edges': edges, 'nodes': nodes}  # type: ignore
