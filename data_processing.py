import logging

import country_converter as coco
import polars as pl

from api import article_disciplines, get_openalex_languages, ojs_articles_by_year
from config import ROR_PATH
from data_loader import download_ror_dataset, get_journals

logger = logging.getLogger(__name__)


def get_journal_options() -> list[dict]:
    """Get list of journals for selection in dashboard."""
    return (
        get_journals()
        .filter(
            pl.col('total_record_count') > 100,
            pl.col('issn').is_not_null(),
            pl.col('issn').str.len_chars() > 7,
        )
        .select('context_name', 'issn')
        .sort('context_name')
        .collect()
        .to_dicts()
    )


def ojs_journals_stats() -> dict[str, int]:
    countries = get_journals().group_by('country').len().collect().shape[0]
    journals = get_journals().collect().shape[0]
    articles = int(ojs_articles_by_year()['count'].sum())
    return {'journals': journals, 'articles': articles, 'countries': countries}


def journal_countries() -> pl.DataFrame:
    return (
        get_journals()
        .group_by('country_code')
        .len()
        .rename({'len': 'count'})
        .filter(pl.col.country_code.str.len_chars() < 3)  # filter out 'not found'
        .collect()
    )


def articles_to_publication_year_stats(
    df: pl.LazyFrame, fractional: bool, ror: str | None = None
) -> pl.DataFrame:
    if fractional and ror is not None:
        # TODO How is this properly calculated? Divide by / sum(author_weight) as well?
        fwci_per_year = (
            df.filter(pl.col('ror') == ror)
            .group_by('publication_year')
            .agg((pl.col('fwci') * pl.col('author_weight')).sum().alias('Count'))
            .rename({'publication_year': 'Publication Year'})
            .with_columns(pl.lit('Fractional weighted FWCI').alias('Dimension'))
            .select('Publication Year', 'Dimension', 'Count')
            .sort('Publication Year')
        )
        authors_works_per_year = (
            df.filter(pl.col('ror') == ror)
            .group_by('publication_year')
            .agg(
                pl.col('author_weight').sum(),
                pl.col('author_id').unique().len().cast(pl.Float64),
            )
            .sort('publication_year')
            .rename(
                {
                    'publication_year': 'Publication Year',
                    'author_weight': 'Fractional Number of Publications',
                    'author_id': 'Number of Authors (Full)',
                }
            )
            .unpivot(
                index='Publication Year', variable_name='Dimension', value_name='Count'
            )
        )
        return pl.concat([authors_works_per_year, fwci_per_year]).collect()
    elif fractional and ror is None:
        raise Exception('Invalid function arguments')
    else:
        fwci_per_year = (
            df.group_by('work_id', 'publication_year')
            .agg(pl.first('fwci').alias('fwci'))
            .group_by('publication_year')
            .agg(pl.col('fwci').sum())
            .rename({'publication_year': 'Publication Year', 'fwci': 'Count'})
            .with_columns(pl.lit('FWCI').alias('Dimension'))
            .select('Publication Year', 'Dimension', 'Count')
            .sort('Publication Year')
        )

        authors_works_per_year = (
            df.group_by('publication_year')
            .agg(
                pl.col('work_id').unique().len().cast(pl.Float64),
                pl.col('author_id').unique().len().cast(pl.Float64),
            )
            .sort('publication_year')
            .rename(
                {
                    'publication_year': 'Publication Year',
                    'work_id': 'Number of Works',
                    'author_id': 'Number of Authors',
                }
            )
            .unpivot(
                index='Publication Year', variable_name='Dimension', value_name='Count'
            )
        )
        return pl.concat(
            [
                authors_works_per_year,
            ]
        ).collect()


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
            .collect()
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
        return pl.concat([df, all_articles]).collect()


def articles_to_ojs_locations(
    df: pl.LazyFrame, fractional: bool, ror: str
) -> pl.DataFrame:
    mark_country_code = get_country_code_for_ror(df, ror)
    journals = (
        get_journals()
        .select('issn', 'country_code', 'country')
        .rename({'country_code': 'journal_country_code', 'country': 'journal_country'})
        .with_columns(pl.col('issn').str.split('\n').alias('issns'))
        .filter(pl.col('issns').is_not_null(), pl.col('issns').list.len() > 0)
    )

    df = (
        df.filter(pl.col('ror') == ror)
        .explode('issn')
        .join(journals.explode('issns'), left_on='issn', right_on='issns')
    )

    if fractional and ror is not None:
        df = df.group_by('journal_country_code', 'journal_country').agg(
            pl.col('author_weight').sum().alias('count')
        )
    else:
        df = df.group_by('journal_country_code', 'journal_country').agg(
            pl.col('work_id').unique().len().alias('count')
        )

    return (
        df.filter(pl.col.journal_country_code.str.len_chars() < 3)
        .with_columns(
            pl.when(pl.col('journal_country_code') == mark_country_code)
            .then(pl.lit('Same country'))
            .otherwise(pl.lit('Different country'))
            .alias('color'),
        )
        .rename({'journal_country': 'Country'})
        .sort('count', descending=True)
        .collect()
    )


def get_country_code_for_ror(articles: pl.LazyFrame, ror: str) -> str:
    return articles.filter(pl.col('ror') == ror).head(1).collect()['country_code'][0]


def articles_to_institution_count(
    articles: pl.LazyFrame, fractional: bool
) -> pl.DataFrame:
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
    else:
        nodes = articles.group_by('ror').len().rename({'len': 'weight'})

    nodes = (
        nodes.join(ror_df, left_on='ror', right_on='id')
        .select('weight', 'name', 'lat', 'lng')
        .rename({'name': 'Institution'})
        .collect()
    )
    return nodes


def get_tops_table(
    df: pl.LazyFrame, fractional: bool = False
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if fractional:
        authors = (
            df.group_by('author_id')
            .agg(
                pl.col('author_weight').sum(),
                pl.col('author_name').first().alias('Author'),
            )
            .sort('author_weight', descending=True)
            .rename({'author_weight': 'Publications'})
            .limit(10)
            .select('Author', 'Publications')
            .collect()
        )

        institutions = (
            df.group_by('ror')
            .agg(pl.col('author_weight').sum(), pl.col('institution_name').first())
            .sort('author_weight', descending=True)
            .rename(
                {
                    'institution_name': 'Institution',
                    'author_weight': 'Publications',
                }
            )
            .limit(10)
            .select('Institution', 'Publications')
            .collect()
        )
    else:
        authors = (
            df.group_by('author_id')
            .agg(pl.col('work_id').len(), pl.col('author_name').first())
            .sort('work_id', descending=True)
            .rename({'author_name': 'Author', 'work_id': 'Publications'})
            .limit(10)
            .select('Author', 'Publications')
            .collect()
        )
        institutions = (
            df.group_by('ror')
            .agg(pl.col('work_id').len(), pl.col('institution_name').first())
            .sort('work_id', descending=True)
            .rename({'institution_name': 'Institution', 'work_id': 'Publications'})
            .limit(10)
            .select('Institution', 'Publications')
            .collect()
        )
    return authors, institutions


def articles_to_author_countries(
    articles: pl.LazyFrame, fractional: bool
) -> pl.DataFrame:
    return (
        articles.group_by('country_code')
        .len()
        .with_columns(pl.col('country_code').str.to_uppercase())
        .rename({'len': 'count'})
        .with_columns(
            pl.col('country_code')
            .alias('Country')
            .map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
        )
        .sort('count', descending=True)
        .collect()
    )


def articles_to_journal_countries(
    articles: pl.LazyFrame, fractional: bool
) -> pl.DataFrame:
    journals = (
        get_journals()
        .select('issn', 'country_code')
        .rename({'country_code': 'journal_country'})
        .with_columns(pl.col('issn').str.split('\n').alias('issns'))
        .filter(pl.col('issns').is_not_null(), pl.col('issns').list.len() > 0)
    )

    return (
        articles.explode('issn')
        .join(journals.explode('issns'), left_on='issn', right_on='issns')
        .group_by('work_id')
        .agg(pl.col('journal_country').first().str.to_uppercase())
        .group_by('journal_country')
        .len()
        .rename({'len': 'count', 'journal_country': 'country_code'})
        .collect()
    )


def articles_to_languages(articles: pl.LazyFrame) -> pl.DataFrame:
    return (
        articles.group_by('language')
        .agg(pl.col('work_id').len().alias('Count'))
        .filter(pl.col('language').is_not_null())
        .collect()
        .join(get_openalex_languages(), left_on='language', right_on='key')
        .select('key_display_name', 'Count')
        .rename({'key_display_name': 'language'})
        .sort('Count', descending=True)
    )
