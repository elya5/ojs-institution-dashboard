import json
from pathlib import Path

import networkx as nx
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from data_processing import (
    articles_to_country_collab_count,
    articles_to_ojs_locations,
)


def get_share_of_ojs_articles_pie(df: pl.DataFrame) -> go.Figure:
    return px.pie(
        df,
        values='Count',
        names='Publisher Software',
        title='Share of Articles in OJS',
    )


def get_ojs_article_count_line(df: pl.DataFrame) -> go.Figure:
    return px.line(
        df,
        x='Publication Year',
        y='Count',
        color='Dimension',
        title='OJS Article per Year',
    )


def get_discipline_bar(df: pl.DataFrame) -> go.Figure:
    return px.bar(
        df,
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


def get_countries_map(df: pl.DataFrame, title: str) -> go.Figure:
    with Path('data/world.json').open() as f:
        geojson = json.load(f)

    countries = pl.DataFrame(
        {
            'country_code': [
                feature['properties']['country_code']
                for feature in geojson['features']
                if 'country_code' in feature['properties']
            ]
        }
    )
    df = countries.join(df, on='country_code', how='left').fill_null(0)

    fig = px.choropleth(
        df,
        geojson=geojson,
        color='count',
        locations='country_code',
        featureidkey='properties.country_code',
        projection='mercator',
    )
    fig.update_geos(visible=False, center={'lat': 20, 'lon': 0}, projection_scale=1)
    fig.update_layout(margin={'r': 0, 't': 30, 'l': 0, 'b': 0}, title=title)
    return fig


def get_institution_map(nodes: pl.DataFrame) -> go.Figure:
    """Get world map with institutes highlighted."""
    node_sizes = 3 + (nodes['weight'] / nodes['weight'].max()) * 10

    fig = go.Figure()
    fig.add_trace(
        go.Scattergeo(
            lon=nodes['lng'],
            lat=nodes['lat'],
            text=nodes['Institution'],
            mode='markers',
            marker={
                'size': node_sizes,
                'color': nodes['weight'],
                'colorscale': 'Viridis',
                'opacity': 0.8,
                'colorbar': {'title': 'Publications'},
            },
            hoverinfo='text',
        )
    )

    fig.update_layout(
        title='Institution Map',
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
