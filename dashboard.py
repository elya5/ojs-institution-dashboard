import logging

import streamlit as st

from api import get_ror_suggestions, ojs_article_for_institution
from data_visualization import (
    get_country_collab_net,
    get_discipline_bar,
    get_institution_collab_map,
    get_ojs_article_count_line,
    get_ojs_journal_locations_bar,
    get_share_of_ojs_articles_pie,
)

st.title('OJS Co-Authorship Dashboard')


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

st.subheader('Institution Information')

st.write('Only articles from 2020 onwards are considered.')

institution_input = st.text_input(
    'Institution Name or ROR', 'https://ror.org/0304hq317'
)
ror_id = None
if 'ror.org' in institution_input:
    ror_id = institution_input
if len(institution_input) > 2:
    options = get_ror_suggestions(institution_input)
    option = st.selectbox(
        'Select institution', options, format_func=lambda option: option['name']
    )
    ror_id = option['ror']

fractional = st.toggle('Use Fractional Counting')

if ror_id is not None and st.button('Analyze'):
    articles = ojs_article_for_institution(ror_id)
    if fractional:
        st.plotly_chart(get_ojs_article_count_line(articles, ror_id, fractional=True))
        st.plotly_chart(get_discipline_bar(articles, ror_id, fractional=True))
        st.warning(
            'Small number of articles below due to testing '
            'with DOAJ and then matching with OJS journals'
        )
        st.plotly_chart(
            get_ojs_journal_locations_bar(articles, ror_id, fractional=True)
        )
        st.plotly_chart(get_country_collab_net(articles, fractional=True))
        st.plotly_chart(get_institution_collab_map(articles, fractional=True))
    else:
        st.plotly_chart(get_share_of_ojs_articles_pie(ror_id))
        st.plotly_chart(get_ojs_article_count_line(articles))
        st.plotly_chart(get_discipline_bar(articles, ror_id))
        st.warning(
            'Small number of articles below due to testing '
            'with DOAJ and then matching with OJS journals'
        )
        st.plotly_chart(get_ojs_journal_locations_bar(articles, ror_id))
        st.plotly_chart(get_country_collab_net(articles))
        st.plotly_chart(get_institution_collab_map(articles))
