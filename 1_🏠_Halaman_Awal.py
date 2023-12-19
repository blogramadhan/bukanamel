#####################################################################################
# Source code: Dashboard Bukan Amel                                                 #
#-----------------------------------------------------------------------------------#
# Dashboard ini dibuat oleh:                                                        #
# Nama          : Kurnia Ramadhan, ST.,M.Eng                                        #
# Jabatan       : Sub Koordinator Pengelolaan Informasi LPSE                        #
# Instansi      : Biro Pengadaan Barang dan Jasa Setda Prov. Kalbar                 #
# Email         : kramadhan@gmail.com                                               #
# URL Web       : https://github.com/blogramadhan                                   #
#-----------------------------------------------------------------------------------#
# Hak cipta milik Allah SWT, source code ini silahkan dicopy, di download atau      #
# di distribusikan ke siapa saja untuk bahan belajar, atau untuk dikembangkan lagi  #
# lebih lanjut, btw tidak untuk dijual ya.                                          #
#                                                                                   #
# Jika teman-teman mengembangkan lebih lanjut source code ini, agar berkenan untuk  #
# men-share code yang teman-teman kembangkan lebih lanjut sebagai bahan belajar     #
# untuk kita semua.                                                                 #
#-----------------------------------------------------------------------------------#
# @ Pontianak, 2023                                                                 #
#####################################################################################

# Import Library
import duckdb
import openpyxl
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
# Import fungsi pribadi
from fungsi import *

st.set_page_config(
    page_title="Dashboard Pengadaan Barang dan Jasa",
    page_icon="👋",
    layout="wide"
)

# App Logo
logo()

st.title("Dashboard Pengadaan Barang dan Jasa")

st.markdown("""
*Dashboard* ini dibuat sebagai alat bantu untuk mempermudah para pelaku pengadaan di seluruh wilayah Provinsi Kalimantan Barat. Data yang disajikan, antara lain:
* **SIRUP**
  * Profil RUP Daerah
  * Profil RUP Perangkat Daerah
  * Struktur Anggaran
  * % Input RUP
  * RUP Paket Penyedia Perangkat Daerah
  * RUP Paket Swakelola Perangkat Daerah
* **SPSE**
  * Tender
    * Pengumuman
    * SPPBJ
    * Kontrak
    * SPMK
    * BAPBAST
  * Non Tender
    * Pengumuman
    * SPPBJ
    * Kontrak
    * SPMK
    * BAPBAST
  * Pencatatan
    * Pencatatan Non Tender
    * Pencatatan Swakelola
  * Peserta Tender
* **PURCHASING**
  * Tramsaksi Katalog
  * Transaksi Toko Daring
* **SIKAP**
* **MONITORING**

Sumber data *Dashboard* ini berasal dari **API JSON Versi 2** yang ditarik harian dari [ISB LKPP](https://lkpp.go.id). 

@2023 - **Kurnia Ramadhan** - LPSE PROV. KALBAR 
""")