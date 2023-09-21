import streamlit as st
import pandas as pd

# Fungsi-fungsi yang bisa digunakan
## Fungsi Download Dataframe ke CSV
def unduh_data(unduhdata):
    return unduhdata.to_excel(index=False).encode('utf')