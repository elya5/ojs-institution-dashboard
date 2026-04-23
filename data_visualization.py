import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from api import share_of_ojs_articles
from data_processing import (
    articles_to_disciplines_count,
    articles_to_publication_year_count,
)


def get_share_of_ojs_articles_pie(ror: str) -> go.Figure:
    return px.pie(
        share_of_ojs_articles(ror),
        values='count',
        names='key_display_name',
        title='Share of Articles in OJS',
    )


def get_ojs_article_count_line(articles: pl.DataFrame) -> go.Figure:
    return px.line(
        articles_to_publication_year_count(articles),
        x='publication_year',
        y='id',
        title='OJS Article per Year',
    )


def get_discipline_bar(articles: pl.DataFrame) -> go.Figure:
    return px.bar(
        articles_to_disciplines_count(articles),
        x='primary_topic:field:display_name',
        y='id',
        title='Articles per Discipline',
    )
