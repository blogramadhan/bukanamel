import streamlit as st
import pandas as pd

# Fungsi-fungsi yang bisa digunakan
## Fungsi Download Dataframe ke CSV
def unduh_data(unduhdata):
    return unduhdata.to_csv(index=False).encode('utf')

@st.cache_data(ttl=(4*3600))
def tarik_data_excel(url):
    return pd.read_excel(url)

@st.cache_data(ttl=(4*3600))
def tarik_data(url):
    return pd.read_parquet(url)