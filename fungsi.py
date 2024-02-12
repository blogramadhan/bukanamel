# Import Library
import duckdb
import openpyxl
import base64
import xlsxwriter
import streamlit as st
import pandas as pd
import numpy as np
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

def download_excel(df):
    # Create a BytesIO object to store Excel file
    excel_data = pd.ExcelWriter("temp.xlsx", engine='xlsxwriter')
    df.to_excel(excel_data, index=False)
    excel_data.save()
    with open("temp.xlsx", "rb") as f:
        excel_bytes = f.read()
    return excel_bytes

@st.cache_data(ttl=(3600))
def tarik_data(url):
    return pd.read_parquet(url)

def logo():
    add_logo("https://storage.googleapis.com/bukanamel/img/instansi-logo.png")