import networkx as nx
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from api import share_of_ojs_articles
from data_processing import (
    articles_to_country_collab_count,
    articles_to_disciplines_count,
    articles_to_institution_collab_count,
    articles_to_ojs_locations,
    articles_to_publication_year_count,
)


def get_share_of_ojs_articles_pie(ror: str) -> go.Figure:
    return px.pie(
        share_of_ojs_articles(ror),
        values='Count',
        names='Publisher Software',
        title='Share of Articles in OJS',
    )


def get_ojs_article_count_line(
    articles: pl.LazyFrame, ror: str | None = None, fractional: bool = False
) -> go.Figure:
    return px.line(
        articles_to_publication_year_count(articles, fractional, ror),
        x='Publication Year',
        y='Count',
        title='OJS Article per Year',
    )


def get_discipline_bar(
    articles: pl.LazyFrame, ror: str, fractional: bool = False
) -> go.Figure:
    return px.bar(
        articles_to_disciplines_count(articles, ror, fractional),
        x='Field',
        y='Count',
        color='Type',
        title='Articles per Discipline',
        barmode='group',
        height=600,
    )


def get_ojs_journal_locations_bar(
    articles: pl.LazyFrame, ror: str, fractional: bool = False
) -> go.Figure:
    return px.bar(
        articles_to_ojs_locations(articles, fractional, ror),
        x='Country Name',
        y='Count',
        color='color',
        title='Articles per OJS journal country',
    )


def __create_network_chart(graph: nx.Graph) -> go.Figure:
    """Create generic network chart based on graph input data."""
    pos = nx.spring_layout(graph, weight='weight', seed=42)

    edge_traces = []
    max_weight = max(d['weight'] for _, _, d in graph.edges(data=True))
    for u, v, d in graph.edges(data=True):
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_traces.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line={'width': 0.1 + d['weight'] / max_weight * 2, 'color': '#888'},
                hoverinfo='none',
                showlegend=False,
            )
        )

    max_weight = max(d['weight'] for _, d in graph.nodes(data=True))
    node_x, node_y, node_text, node_size = [], [], [], []
    for node, data in graph.nodes(data=True):
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(f'{node}<br>Total collabs: {data["weight"]}')
        node_size.append(5 + (2 * data['weight'] / max_weight))

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
            'opacity': 1,
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


def get_country_collab_net(
    articles: pl.LazyFrame, fractional: bool = False
) -> go.Figure:
    collabs = articles_to_country_collab_count(articles, fractional)
    G = nx.Graph()
    for row in collabs['edges'].iter_rows(named=True):
        G.add_edge(row['country_a'], row['country_b'], weight=row['count'])
    for row in collabs['nodes'].iter_rows(named=True):
        G.add_node(row['country'], weight=row['weight'])

    fig = __create_network_chart(G)
    fig.layout.title = 'Network of Country Collaborations'

    return fig


def get_institution_collab_map(
    articles: pl.LazyFrame, fractional: bool = False
) -> go.Figure:
    """Get world map with co-authoring institutes highlighted."""
    threshold = 1_000
    collabs = articles_to_institution_collab_count(articles, fractional)
    edges = collabs['edges'][:threshold]

    nodes = collabs['nodes'].filter(
        pl.col('Institution').is_in(edges['Institution 1'].implode())
        | pl.col('Institution').is_in(edges['Institution 2'].implode()),
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scattergeo(
            lon=nodes['lng'],
            lat=nodes['lat'],
            text=nodes['Institution'],
            mode='markers',
            marker={'size': 4, 'color': 'rgba(0, 100, 125)', 'opacity': 0.9},
            hoverinfo='text',
        )
    )

    max_collab = edges['count'].max()

    for row in edges.iter_rows(named=True):
        fig.add_trace(
            go.Scattergeo(
                lon=[row['lng1'], row['lng2']],
                lat=[row['lat1'], row['lat2']],
                mode='lines',
                line={
                    'width': 0.5 + 5 * (row['count'] / max_collab),
                    'color': 'rgba(0, 100, 255, 0.4)',
                },
                opacity=0.6,
                hoverinfo='none',
            )
        )

    fig.update_layout(
        title=f'OJS Institution Co-Authorship Network (top {threshold})',
        showlegend=False,
        geo={
            'scope': 'world',
            'projection_type': 'natural earth',
            'showland': True,
            'landcolor': 'rgb(240, 240, 240)',
            'countrycolor': 'rgb(200, 200, 200)',
            'showocean': True,
            'oceancolor': 'rgb(220, 235, 255)',
        },
        height=600,
    )

    return fig
