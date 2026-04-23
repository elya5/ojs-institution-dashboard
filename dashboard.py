import logging

import streamlit as st

from api import ojs_article_for_institution
from data_processing import get_journals
from data_visualization import (
    get_discipline_bar,
    get_ojs_article_count_line,
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
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logging.getLogger('country_converter').propagate = False


journals_df = get_journals()

st.subheader('Institution Information')

st.write('Only articles from 2020 onwards are considered.')

ror_id = st.text_input('ROR', 'https://ror.org/0304hq317')

if st.button('Analyze'):
    articles = ojs_article_for_institution(ror_id)

    st.plotly_chart(get_share_of_ojs_articles_pie(ror_id))
    st.plotly_chart(get_ojs_article_count_line(articles))
    st.plotly_chart(get_discipline_bar(articles))
