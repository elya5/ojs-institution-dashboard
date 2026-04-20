import logging
from itertools import combinations

import country_converter as coco
import polars as pl
from data_loader import download_openalex_journal_articles

logger = logging.getLogger(__name__)


def get_journals() -> pl.DataFrame:
    """Get a dataframe of all OJS journals in the beacon.csv"""
    logger.info('Reading OJS journals.')
    # TODO: filter out journals with less that X articles?
    return pl.read_csv('data/beacon.csv')


def get_articles(journals: pl.DataFrame) -> pl.DataFrame:
    """Fetch articles for journals from OpenAlex as DataFrame."""
    logger.info('Fetching articles from OpenAlex.')
    result = []
    for journal in journals.iter_rows(named=True):
        articles = download_openalex_journal_articles(journal['issn'].split('\n'))
        for article in articles:
            article['journal_country_code'] = journal['country_consolidated']
        result.extend(articles)
    return pl.DataFrame(result)


def openalex_to_author_df(articles: pl.DataFrame) -> pl.DataFrame:
    """
    Convert a DataFrame with articles to an exploded DF with authors.

    Remaining columns are:
     - work_id
     - publication_year
     - cited_by_count
     - type
     - id
     - display_name
     - institutions_id
     - institutions_display_name
     - institutions_country_code
     - institutions_continent
     - journal_country_code
    """
    # TODO simplification: currently only takes authors' first institution
    #      entry and with known institutions entry
    logger.info('Converting OpenAlex data.')
    ror_continents = (
        pl.read_csv('data/ror_data.csv')
        .rename(
            {
                'locations.geonames_details.continent_name': 'institutions_continent',
                'id': 'ror_id',
            }
        )
        .select(['ror_id', 'institutions_continent'])
    )
    return (
        articles.select(
            [
                'id',
                'publication_year',
                'cited_by_count',
                'type',
                'authorships',
                'journal_country_code',
            ]
        )
        .rename({'id': 'work_id'})
        .explode('authorships')
        .unnest('authorships')
        .unnest('author')
        .drop(
            [
                'is_corresponding',
                'author_position',
                'orcid',
                'raw_author_name',
                'raw_affiliation_strings',
                'countries',
                'affiliations',
            ]
        )
        .filter(pl.col('institutions').list.len() > 0)
        .with_columns(pl.col('institutions').list.get(0))
        .unnest('institutions', separator='_')
        .join(ror_continents, left_on='institutions_ror', right_on='ror_id')
        .drop(['institutions_lineage', 'institutions_type', 'institutions_ror'])
    )


def __group_to_collab_count(author_articles: pl.DataFrame, column: str) -> pl.DataFrame:
    """Convert DataFrame to count of collaborating pairs by column (e.g. country)."""
    return (
        author_articles.group_by('work_id')
        .agg(pl.col(column).unique().sort().drop_nulls())
        .filter(pl.col(column).list.len() >= 2)
        .with_columns(
            pl.col(column)
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
    )


def authors_to_country_collab_count(author_articles: pl.DataFrame) -> pl.DataFrame:
    """Convert DataFrame to count of collaborating country pairs."""
    logger.info('Generating country collaboration data.')

    def coco_con(countrycodes: pl.Series) -> pl.Series:
        return pl.Series(coco.convert(countrycodes, to='name_short'))

    df = __group_to_collab_count(author_articles, 'institutions_country_code')
    return df.with_columns(
        pl.col('a').alias('country_a').map_batches(coco_con),
        pl.col('b').alias('country_b').map_batches(coco_con),
    ).drop(['a', 'b'])


def authors_to_continent_collab_count(author_articles: pl.DataFrame) -> pl.DataFrame:
    """Convert DataFrame to count of collaborating continent pairs."""
    logger.info('Generating continent collaboration data.')
    return __group_to_collab_count(author_articles, 'institutions_continent').rename(
        {
            'a': 'continent_a',
            'b': 'continent_b',
        }
    )


def authors_to_country_collabs_share(author_articles_df: pl.DataFrame) -> pl.DataFrame:
    """Convert DataFrame to share of international country collaborations."""
    logger.info('Generating international collaboration data.')
    return (
        author_articles_df.group_by('work_id')
        .agg(pl.col('institutions_country_code').unique())
        .with_columns(
            pl.when(pl.col('institutions_country_code').list.len() > 1)
            .then(pl.lit('International Collaboration'))
            .otherwise(pl.lit('Single Country'))
            .alias('Collaboration')
        )
        .group_by('Collaboration')
        .len()
        .rename({'len': 'Count'})
    )


def authors_to_authors_count(author_articles_df: pl.DataFrame) -> pl.DataFrame:
    """Convert DataFrame to count of articles with X authors."""
    logger.info('Generating author count data.')
    return (
        author_articles_df.group_by('work_id')
        .len()
        .rename({'len': 'Number of Authors'})
        .group_by('Number of Authors')
        .len()
        .rename({'len': 'Count'})
    )


def authors_to_author_as_journal_location_share(df: pl.DataFrame) -> pl.DataFrame:
    """Calcuate share of articles where author country and journal country are equal."""
    return (
        df.group_by('work_id')
        .agg(
            pl.col('institutions_country_code').drop_nulls(),
            pl.col('journal_country_code').unique(),
        )
        .with_columns(
            pl.when(
                pl.col('institutions_country_code').list.contains(
                    pl.col('journal_country_code').list.get(0)
                )
            )
            .then(pl.lit('Same Country'))
            .otherwise(pl.lit('Different Country'))
            .alias('Country')
        )
        .group_by('Country')
        .len()
        .rename({'len': 'Count'})
    )
