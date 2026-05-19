from contextlib import asynccontextmanager

from cashews import cache
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from api import (
    author_countries,
    get_openalex_countries,
    get_ror_suggestions,
    get_tops_table_country,
    ojs_article_for_institution,
    ojs_articles_by_year,
    ojs_articles_for_journal,
    publication_per_discipline_country,
    publication_to_journal_location_country,
    publications_per_institutions_locations,
    publications_per_year_country,
    share_of_ojs_articles,
    share_of_ojs_articles_country,
)
from data_processing import (
    articles_to_author_countries,
    articles_to_disciplines_count,
    articles_to_institution_count,
    articles_to_ojs_locations,
    articles_to_publication_year_stats,
    get_journal_options,
    get_tops_table,
    ojs_journals_stats,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache.setup('mem://')
    yield
    await cache.close()


app = FastAPI(
    title='OJS Dashboard API',
    description='JSON API for the OJS publication analytics dashboard.',
    version='1.0.0',
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['GET'],
    allow_headers=['*'],
)


@app.get('/journals', summary='List all available journals')
@cache(ttl='24h')
async def list_journals():
    return get_journal_options()


@app.get(
    '/all-articles-yearly', summary='Get how many OJS articles were published yearly'
)
@cache(ttl='24h')
async def get_all_ojs_articles_per_year():
    return ojs_articles_by_year().to_dicts()


@app.get('/get-ojs-journals-stats', summary='Get counts for all OJS journals/articles')
def get_ojs_journals_stats():
    return ojs_journals_stats()


@app.get('/list-countries', summary='List all countries in OpenAelx')
@cache(ttl='24h')
async def list_countries():
    return get_openalex_countries()


@app.get(
    '/country-analysis/{country_code}',
    summary='Get full analysis results for one country',
)
@cache(ttl='24h')
async def country_analsis(country_code: str):
    table_authors, table_institutions = get_tops_table_country(country_code)
    return {
        'ojs_share': share_of_ojs_articles_country(country_code).to_dicts(),
        'yearly_articles': publications_per_year_country(country_code).to_dicts(),
        'disciplines': publication_per_discipline_country(country_code).to_dicts(),
        'institutions': publications_per_institutions_locations(
            country_code
        ).to_dicts(),
        'journal_locations': publication_to_journal_location_country(
            country_code
        ).to_dicts(),
        'top_authors': table_authors.to_dicts(),
        'top_institutions': table_institutions.to_dicts(),
        'countries': author_countries(country_code).to_dicts(),
    }


@app.get('/institution-analysis/', summary='Get full analysis results for one country')
@cache(ttl='24h')
async def institution_analsis(
    fractional: bool = Query(..., description='Whether fractional or full counting'),
    institution: str = Query(..., description='Institution to analyse'),
):
    articles = ojs_article_for_institution(institution)

    table_authors, table_institutions = get_tops_table(articles, fractional=fractional)

    ojs_share = None
    if not fractional:
        ojs_share = share_of_ojs_articles(institution).to_dicts()

    return {
        'yearly_stats': articles_to_publication_year_stats(
            articles, fractional, institution
        ).to_dicts(),
        'disciplines': articles_to_disciplines_count(
            articles, institution, fractional
        ).to_dicts(),
        'journal_locations': articles_to_ojs_locations(
            articles, fractional, institution
        ).to_dicts(),
        'institutions': articles_to_institution_count(
            articles, fractional=fractional
        ).to_dicts(),
        'ojs_share': ojs_share,
        'top_authors': table_authors.to_dicts(),
        'top_institutions': table_institutions.to_dicts(),
    }


@app.get('/institutions/suggest', summary='ROR auto-complete suggestions')
@cache(ttl='200h')
async def suggest_institutions(
    q: str = Query(..., min_length=3, description='Partial institution name'),
):
    """Returns ROR suggestions for a partial institution name string."""
    return get_ror_suggestions(q)


@app.get('/journal-analysis/', summary='Get full analysis results for one journal')
@cache(ttl='24h')
async def journal_analsis(
    fractional: bool = Query(..., description='Whether fractional or full counting'),
    journal: str = Query(..., description='ISSNs of journals to analyse'),
):
    articles = ojs_articles_for_journal(journal.split('\n'))
    table_authors, table_institutions = get_tops_table(articles, fractional=fractional)
    return {
        'yearly_stats': articles_to_publication_year_stats(
            articles, False, None
        ).to_dicts(),
        'top_authors': table_authors.to_dicts(),
        'top_institutions': table_institutions.to_dicts(),
        'author_countries': articles_to_author_countries(
            articles, fractional
        ).to_dicts(),
        'institutions': articles_to_institution_count(articles, fractional).to_dicts(),
    }
