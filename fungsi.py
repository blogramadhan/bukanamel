# Import Library
import duckdb
import openpyxl
import streamlit as st
import pandas as pd
import plotly.express as px
# Import library currency
from babel.numbers import format_currency
# Import library Aggrid
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
# Import Streamlit Extras
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.app_logo import add_logo

# Fungsi-fungsi yang bisa digunakan
## Fungsi Download Dataframe ke CSV
def unduh_data(unduhdata):
    return unduhdata.to_csv(index=False).encode('utf-8')

@st.cache_data(ttl=(6*3600))
def tarik_data_excel(url):
    return pd.read_excel(url)

@st.cache_data(ttl=(6*3600))
def tarik_data(url):
    return pd.read_parquet(url)

def logo():
    add_logo("https://storage.googleapis.com/bukanamel/img/instansi-logo.png")