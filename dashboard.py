import logging

import streamlit as st

from api import (
    get_openalex_countries,
    get_ror_suggestions,
    get_tops_table_country,
    ojs_article_for_institution,
    ojs_articles_for_journal,
    publication_per_discipline_country,
    publication_to_journal_location_country,
    publications_per_institutions_locations,
    publications_per_year_country,
    share_of_ojs_articles,
    share_of_ojs_articles_country,
)
from config import MIN_PUBLICATION_YEAR
from data_processing import (
    articles_to_author_countries,
    articles_to_disciplines_count,
    articles_to_institution_count,
    articles_to_journal_countries,
    articles_to_publication_year_stats,
    get_journal_options,
    get_tops_table,
)
from data_visualization import (
    get_countries_map,
    get_country_collab_net,
    get_discipline_bar,
    get_institution_map,
    get_ojs_article_count_line,
    get_ojs_journal_locations_bar,
    get_share_of_ojs_articles_pie,
)

st.title('OJS Dashboard')


class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func
        self.logs = []

    def emit(self, record):
        msg = self.format(record)
        self.logs.append(msg)
        self.widget_update_func('Logs', '\n'.join(self.logs[::-1]))


formatter = logging.Formatter('%(asctime)s - %(message)s')
handler = StreamlitLogHandler(st.empty().text_area)
handler.setFormatter(formatter)
handler_console = logging.StreamHandler()
handler_console.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.addHandler(handler_console)
logging.getLogger('country_converter').propagate = False

logger.info('Initializing')

analysis_level = st.selectbox(
    'Select the analysis level',
    ('Institution', 'Journal', 'Country'),
)
if analysis_level == 'Institution':
    st.subheader('Institution Analysis')

    st.write(f'Only articles from {MIN_PUBLICATION_YEAR} onwards are considered.')

    institution_input = st.text_input(
        'Institution Name or ROR', 'https://ror.org/0304hq317'
    )
    ror_id = None
    if 'ror.org' in institution_input:
        ror_id = institution_input
    elif len(institution_input) > 2:
        options = get_ror_suggestions(institution_input)
        option = st.selectbox(
            'Select institution', options, format_func=lambda option: option['name']
        )
        ror_id = option['ror']

    fractional = st.toggle('Use Fractional Counting')

    if ror_id is not None and st.button('Analyze'):
        articles = ojs_article_for_institution(ror_id)
        if fractional:
            st.plotly_chart(
                get_ojs_article_count_line(
                    articles_to_publication_year_stats(articles, True, ror_id)
                )
            )
            st.plotly_chart(
                get_discipline_bar(
                    articles_to_disciplines_count(articles, ror_id, True)
                )
            )
            st.warning(
                'Small number of articles below due to testing '
                'with DOAJ and then matching with OJS journals'
            )
            st.plotly_chart(
                get_ojs_journal_locations_bar(articles, ror_id, fractional=True)
            )
            st.markdown('**Top Authors and Institutions**')
            table_authors, table_institutions = get_tops_table(
                articles, fractional=True
            )
            col1, col2 = st.columns(2)
            col1.dataframe(table_authors)
            col2.dataframe(table_institutions)
            st.plotly_chart(
                get_institution_map(
                    articles_to_institution_count(articles, fractional=True)
                )
            )
        else:
            st.plotly_chart(
                get_share_of_ojs_articles_pie(share_of_ojs_articles(ror_id))
            )
            st.plotly_chart(
                get_ojs_article_count_line(
                    articles_to_publication_year_stats(articles, False, None)
                )
            )
            st.plotly_chart(
                get_discipline_bar(
                    articles_to_disciplines_count(articles, ror_id, False)
                )
            )
            st.warning(
                'Small number of articles below due to testing '
                'with DOAJ and then matching with OJS journals'
            )
            st.plotly_chart(get_ojs_journal_locations_bar(articles, ror_id))
            st.markdown('**Top Authors and Institutions**')
            table_authors, table_institutions = get_tops_table(articles)
            col1, col2 = st.columns(2)
            col1.dataframe(table_authors)
            col2.dataframe(table_institutions)
            st.plotly_chart(
                get_institution_map(
                    articles_to_institution_count(articles, fractional=False)
                )
            )
            st.plotly_chart(
                get_countries_map(
                    articles_to_journal_countries(articles, False), 'Journal Countries'
                )
            )

elif analysis_level == 'Country':
    st.subheader('Country Analysis')
    country = st.selectbox(
        'Select the Country',
        get_openalex_countries(),
        format_func=lambda c: c['name'],
        index=None,
    )
    if country is not None and st.button('Analyze'):
        country_code = country['key']
        st.plotly_chart(
            get_share_of_ojs_articles_pie(share_of_ojs_articles_country(country_code))
        )
        st.plotly_chart(
            get_ojs_article_count_line(publications_per_year_country(country_code))
        )

        st.markdown('**Top Authors and Institutions**')
        table_authors, table_institutions = get_tops_table_country(country_code)
        col1, col2 = st.columns(2)
        col1.dataframe(table_authors)
        col2.dataframe(table_institutions)
        st.plotly_chart(
            get_discipline_bar(publication_per_discipline_country(country_code))
        )
        st.warning('limited to only top 200 rors')
        st.plotly_chart(
            get_institution_map(publications_per_institutions_locations(country_code))
        )
        st.warning('limited to only top 200 issns')
        st.plotly_chart(
            get_countries_map(
                publication_to_journal_location_country(country_code),
                'Journal Countries',
            )
        )


elif analysis_level == 'Journal':
    st.subheader('Journal Analysis')
    st.warning('TODO currently fetches all -> only in beacon years?')

    journal = st.selectbox(
        'Select a journal to analyze',
        get_journal_options(),
        index=None,
        format_func=lambda j: j['context_name'],
    )

    fractional = st.toggle('Use Fractional Counting')

    if journal is not None and st.button('Analyze'):
        articles = ojs_articles_for_journal(journal['issn'].split('\n'))
        if fractional:
            st.text(
                'Full and fractional counting are the same for this line chart below.'
            )
            st.plotly_chart(
                get_ojs_article_count_line(
                    articles_to_publication_year_stats(articles, False, None)
                )
            )
            st.markdown('**Top Authors and Institutions**')
            table_authors, table_institutions = get_tops_table(
                articles, fractional=True
            )
            col1, col2 = st.columns(2)
            col1.dataframe(table_authors)
            col2.dataframe(table_institutions)
            st.plotly_chart(
                get_countries_map(
                    articles_to_author_countries(articles, True), 'Author Countries'
                )
            )
            st.plotly_chart(get_country_collab_net(articles, fractional=True))
            st.plotly_chart(
                get_institution_map(
                    articles_to_institution_count(articles, fractional=True)
                )
            )
        else:
            st.plotly_chart(
                get_ojs_article_count_line(
                    articles_to_publication_year_stats(articles, False, None)
                )
            )
            st.markdown('**Top Authors and Institutions**')
            table_authors, table_institutions = get_tops_table(articles)
            col1, col2 = st.columns(2)
            col1.dataframe(table_authors)
            col2.dataframe(table_institutions)
            st.plotly_chart(
                get_countries_map(
                    articles_to_author_countries(articles, False), 'Author Countries'
                )
            )
            st.plotly_chart(get_country_collab_net(articles))
            st.plotly_chart(
                get_institution_map(
                    articles_to_institution_count(articles, fractional=False)
                )
            )
