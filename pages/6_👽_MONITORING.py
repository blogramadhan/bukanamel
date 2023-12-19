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
import plotly.express as px
# Import library currency
from babel.numbers import format_currency
# Import library Aggrid
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
# Import Streamlit Extras
from streamlit_extras.metric_cards import style_metric_cards
# Import fungsi pribadi
from fungsi import *

# App Logo
logo()

# Konfigurasi variabel lokasi UKPBJ
daerah =    ["PROV. KALBAR", "KAB. BENGKAYANG", "KAB. MELAWI", "KOTA PONTIANAK", "KAB. SANGGAU", "KAB. SEKADAU", "KAB. KAPUAS HULU", "KAB. KUBU RAYA", "KAB. LANDAK", "KOTA SINGKAWANG", 
             "KAB. SINTANG", "KAB. MEMPAWAH", "KAB. KETAPANG", "KAB. KATINGAN"]

tahuns = ["2023", "2022"]

pilih = st.sidebar.selectbox("Pilih UKPBJ yang diinginkan :", daerah)
tahun = st.sidebar.selectbox("Pilih Tahun :", tahuns)

if pilih == "PROV. KALBAR":
    kodeFolder = "prov"
    kodeInstansi = "D197"
if pilih == "KAB. BENGKAYANG":
    kodeFolder = "bky"
    kodeInstansi = "D206"
if pilih == "KAB. MELAWI":
    kodeFolder = "mlw"
    kodeInstansi = "D210"
if pilih == "KOTA PONTIANAK":
    kodeFolder = "ptk"
    kodeInstansi = "D199"
if pilih == "KAB. SANGGAU":
    kodeFolder = "sgu"
    kodeInstansi = "D204"
if pilih == "KAB. SEKADAU":
    kodeFolder = "skd"
    kodeInstansi = "D198"
if pilih == "KAB. KAPUAS HULU":
    kodeFolder = "kph"
    kodeInstansi = "D209"
if pilih == "KAB. KUBU RAYA":
    kodeFolder = "kkr"
    kodeInstansi = "D202"
if pilih == "KAB. LANDAK":
    kodeFolder = "ldk"
    kodeInstansi = "D205"
if pilih == "KOTA SINGKAWANG":
    kodeFolder = "skw"
    kodeInstansi = "D200"
if pilih == "KAB. SINTANG":
    kodeFolder = "stg"
    kodeInstansi = "D211"
if pilih == "KAB. MEMPAWAH":
    kodeFolder = "mpw"
    kodeInstansi = "D552"
if pilih == "KAB. KETAPANG":
    kodeFolder = "ktp"
    kodeInstansi = "D201"
if pilih == "KAB. KATINGAN":
    kodeFolder = "ktn"
    kodeInstansi = "D236"

# Persiapan Dataset
con = duckdb.connect(database=':memory:')

## Akses file dataset format parquet dari Google Cloud Storage via URL public

### Dataset SPSE Tender dan SIKAP
DatasetSPSETenderPengumuman = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-TenderPengumuman{tahun}.parquet"
DatasetSIKAPTender = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sikap/SIKAPPenilaianKinerjaPenyediaTender{tahun}.parquet"

### Dataset SPSE Non Tender dan SIKAP
DatasetSPSENonTenderPengumuman = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-NonTenderPengumuman{tahun}.parquet"
DatasetSIKAPNonTender = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sikap/SIKAPPenilaianKinerjaPenyediaNonTender{tahun}.parquet"

#####
# Mulai membuat presentasi data Purchasing
#####

# Buat menu yang mau disajikan
menu_monitoring_1, menu_monitoring_2 = st.tabs(["| ITKP |", "| SIKAP |"])

## Tab menu monitoring ITKP
with menu_monitoring_1:

    st.title("MENU ITKP")

with menu_monitoring_2:

    st.header(f"MONITORING SIKAP - {pilih} - TAHUN {tahun}")

    ### Buat sub menu SIKAP
    menu_monitoring_2_1, menu_monitoring_2_2 = st.tabs(["| SIKAP TENDER |", "| SIKAP NON TENDER |"])

    #### Tab menu SIKAP - TENDER
    with menu_monitoring_2_1:

        try:
            ##### Tarik dataset SIKAP TENDER
            df_SPSETenderPengumuman = tarik_data(DatasetSPSETenderPengumuman)
            df_SIKAPTender = tarik_data(DatasetSIKAPTender)

            ##### Buat tombol undah dataset SIKAP TENDER
            unduh_SIKAP_Tender = unduh_data(df_SIKAPTender)

            SIKAP_Tender_1, SIKAP_Tender_2 = st.columns((7,3))
            with SIKAP_Tender_1:
                st.subheader("SIKAP TENDER")
            with SIKAP_Tender_2:
                st.download_button(
                    label = "📥 Download Data SIKAP Tender",
                    data = unduh_SIKAP_Tender,
                    file_name = f"SIKAPTender-{kodeFolder}-{tahun}.csv",
                    mime = "text/csv"
                )



        except Exception:
            st.error("Gagal baca dataset SIKAP TENDER")

    with menu_monitoring_2_2:

        try:
            ##### Tarik dataset SIKAP NON TENDER
            df_SPSENonTenderPengumuman = tarik_data(DatasetSPSENonTenderPengumuman)
            df_SIKAPNonTender = tarik_data(DatasetSIKAPNonTender)

            ##### Buat tombol undah dataset SIKAP TENDER
            unduh_SIKAP_NonTender = unduh_data(df_SIKAPNonTender)

            SIKAP_NonTender_1, SIKAP_NonTender_2 = st.columns((7,3))
            with SIKAP_NonTender_1:
                st.subheader("SIKAP NON TENDER")
            with SIKAP_NonTender_2:
                st.download_button(
                    label = "📥 Download Data SIKAP Non Tender",
                    data = unduh_SIKAP_NonTender,
                    file_name = f"SIKAPNonTender-{kodeFolder}-{tahun}.csv",
                    mime = "text/csv"
                )        

        except Exception:
            st.error("Gagal baca dataset SIKAP NON TENDER")