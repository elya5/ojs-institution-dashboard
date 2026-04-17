from itertools import combinations

import country_converter as coco
import polars as pl

from data_loader import download_openalex_journal_articles


def get_journals() -> pl.DataFrame:
    """Get a dataframe of all OJS journals in the beacon.csv"""
    # TODO: filter out journals with less that X articles?
    return pl.read_csv('beacon.csv')


def get_articles(journals: pl.DataFrame) -> pl.DataFrame:
    result = []
    for journal in journals.iter_rows(named=True):
        articles = download_openalex_journal_articles(journal['issn'].split('\n'))
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
     - institutions_ror
     - institutions_country_code
    """
    # TODO simplification: currently only takes authors' first institution entry and with known institutions entry
    return (articles
        .select(['id', 'publication_year', 'cited_by_count', 'type', 'authorships'])
        .rename({'id': 'work_id'})
        .explode('authorships')
        .unnest('authorships')
        .unnest('author')
        .drop(['is_corresponding', 'author_position', 'orcid', 'raw_author_name', 'raw_affiliation_strings', 'countries', 'affiliations'])
        .filter(pl.col('institutions').list.len() > 0)
        .with_columns(pl.col('institutions').list.get(0))
        .unnest('institutions', separator='_')
        .drop(['institutions_lineage', 'institutions_type'])
    )


def authors_to_country_collab_count(author_articles: pl.DataFrame) -> pl.DataFrame:
    """Convert DataFrame with authors per article to collaboration countries count."""
    return (author_articles
        .group_by('work_id')
        .agg(pl.col('institutions_country_code').unique().sort().drop_nulls())
        .filter(pl.col('institutions_country_code').list.len()>=2)
        .with_columns(
            pl.col('institutions_country_code')
                .map_elements(lambda codes: list(combinations(codes, 2)),
                              return_dtype=pl.List(pl.List(pl.String))
                )
                .alias('country_pairs')
        )
        .explode('country_pairs')
        .group_by('country_pairs')
        .len()
        .sort('len', descending=True)
        .with_columns(
            pl.col('country_pairs').list.get(0).alias('country_a').map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
            pl.col('country_pairs').list.get(1).alias('country_b').map_batches(lambda x: pl.Series(coco.convert(x, to='name_short'))),
        )
        .rename({'len': 'count'})
    )

def authors_to_country_collabs(author_articles_df: pl.DataFrame) -> pl.DataFrame:
    """Convert DataFrame with authors per article to share of international country collaborations."""
    return (author_articles_df
        .group_by('work_id')
        .agg(pl.col('institutions_country_code').unique())
        .with_columns(
            pl.when(pl.col("institutions_country_code").list.len() > 1)
              .then(pl.lit("International Collaboration"))
              .otherwise(pl.lit("Single Country"))
              .alias("Collaboration")
        ).group_by("Collaboration")
        .len()
        .rename({'len': 'Count'})
    )
