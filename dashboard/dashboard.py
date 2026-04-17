import polars as pl
import streamlit as st

from data_processing import get_journals, get_articles, openalex_to_author_df
from data_visualization import get_country_collab_networkchart, get_international_collab_piechart


st.title('OJS Co-Authorship Dashboard')
journals_df = get_journals()

st.subheader('Journal Information')

option = st.selectbox(
    "Select a journal to analyze",
    [(j['context_name'], ) for j in journals_df.iter_rows(named=True)],
    format_func=lambda r: r[0]
)

selected_df = journals_df.filter(pl.col('context_name') == option[0]).head(1)

st.write("You selected:", selected_df)

if st.button('Analyze'):
    num_records = selected_df['total_record_count'][0]
    if num_records > 2_000:
        st.write(f'Too many records to fetch: {num_records}')
    else:
        st.write('Fetching articles from OpenAlex')
        author_articles_df = openalex_to_author_df(get_articles(selected_df))
        st.plotly_chart(get_international_collab_piechart(author_articles_df))
        st.plotly_chart(get_country_collab_networkchart(author_articles_df))
