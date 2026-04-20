import networkx as nx
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from data_processing import (
    authors_to_authors_count,
    authors_to_continent_collab_count,
    authors_to_country_collab_count,
    authors_to_country_collabs,
)


def get_international_collab_piechart(author_articles_df: pl.DataFrame) -> go.Figure:
    """Create pie chart with international collab vs. just one country articles."""
    collabs = authors_to_country_collabs(author_articles_df)
    return px.pie(
        collabs,
        values='Count',
        names='Collaboration',
        title='Share of International Collaboration in Articles',
    )


def create_network_chart(graph: nx.Graph) -> go.Figure:
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


def get_country_collab_networkchart(author_articles_df: pl.DataFrame) -> go.Figure:
    """Create network graph with edges for collaborating countries."""
    collabs = authors_to_country_collab_count(author_articles_df)
    G = nx.Graph()
    for row in collabs.iter_rows(named=True):
        G.add_edge(row['country_a'], row['country_b'], weight=row['count'])

    fig = create_network_chart(G)
    fig.layout.title = 'Network of Country Collaborations'

    return fig


def get_continent_collab_networkchart(author_articles_df: pl.DataFrame) -> go.Figure:
    """Create network graph with edges for collaborating continents."""
    collabs = authors_to_continent_collab_count(author_articles_df)
    G = nx.Graph()
    for row in collabs.iter_rows(named=True):
        G.add_edge(row['continent_a'], row['continent_b'], weight=row['count'])

    fig = create_network_chart(G)
    fig.layout.title = 'Network of Continent Collaborations'

    return fig


def get_number_of_authors_bar_chart(author_articles_df: pl.DataFrame) -> go.Figure:
    """Create bar chart with articles by number of authors."""
    num_authors = authors_to_authors_count(author_articles_df)
    return px.bar(
        num_authors,
        x='Number of Authors',
        y='Count',
        title='Articles by Number of Authors',
    )
