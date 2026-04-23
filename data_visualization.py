import networkx as nx
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from api import share_of_ojs_articles
from data_processing import (
    articles_to_disciplines_count,
    articles_to_ojs_locations,
    articles_to_publication_year_count,
    authors_to_country_collab_count,
    get_country_code_for_ror,
)


def get_share_of_ojs_articles_pie(ror: str) -> go.Figure:
    return px.pie(
        share_of_ojs_articles(ror),
        values='count',
        names='key_display_name',
        title='Share of Articles in OJS',
    )


def get_ojs_article_count_line(articles: pl.LazyFrame) -> go.Figure:
    return px.line(
        articles_to_publication_year_count(articles),
        x='publication_year',
        y='id',
        title='OJS Article per Year',
    )


def get_discipline_bar(articles: pl.LazyFrame, ror: str) -> go.Figure:
    return px.bar(
        articles_to_disciplines_count(articles, ror),
        x='Field',
        y='count',
        color='Type',
        title='Articles per Discipline',
        barmode='group',
        height=600,
    )


def get_ojs_journal_locations_bar(articles: pl.LazyFrame, ror: str) -> go.Figure:
    country_code = get_country_code_for_ror(articles, ror)
    return px.bar(
        articles_to_ojs_locations(articles, country_code),
        x='country_name',
        y='id',
        color='color',
        title='Articles per OJS journal country',
    )


def __create_network_chart(graph: nx.Graph) -> go.Figure:
    """Create generic network chart based on graph input data."""
    pos = nx.spring_layout(graph, weight='weight', seed=42)

    edge_traces = []
    for u, v in graph.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_traces.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line={'width': 1, 'color': '#888'},
                hoverinfo='none',
                showlegend=False,
            )
        )

    node_x, node_y, node_text, node_size = [], [], [], []
    max_weight = max(d['weight'] for _, _, d in graph.edges(data=True))
    for node in graph.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        degree = sum(d['weight'] for _, _, d in graph.edges(node, data=True))
        node_text.append(f'{node}<br>Total collabs: {degree}')
        node_size.append(5 + (2 * degree / max_weight))

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers+text',
        text=list(graph.nodes()),
        textposition='top center',
        hovertext=node_text,
        hoverinfo='text',
        marker={
            'size': node_size,
            'color': node_size,
            'colorscale': 'Viridis',
            'showscale': True,
            'colorbar': {'title': 'Collaboration strength'},
            'line': {'width': 1, 'color': 'white'},
        },
        showlegend=False,
    )

    fig = go.Figure(
        data=[*edge_traces, node_trace],
        layout=go.Layout(
            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
            hovermode='closest',
            margin={'l': 20, 'r': 20, 't': 20, 'b': 20},
        ),
    )

    return fig


def get_country_collab_net(articles: pl.LazyFrame):
    collabs = authors_to_country_collab_count(articles)
    G = nx.Graph()
    for row in collabs.iter_rows(named=True):
        G.add_edge(row['country_a'], row['country_b'], weight=row['count'])

    fig = __create_network_chart(G)
    fig.layout.title = 'Network of Country Collaborations'

    return fig
