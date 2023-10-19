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
import streamlit as st
import pandas as pd
import plotly.express as px
# Import library currency
from babel.numbers import format_currency
# Import library Aggrid
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
# Import Streamlit Extras
#from streamlit_extras.metric_cards import style_metric_cards
# Import fungsi pribadi
from fungsi import *

# Konfigurasi variabel lokasi UKPBJ
daerah = ["PROV. KALBAR", "KAB. BENGKAYANG", "KAB. MELAWI", "KOTA PONTIANAK", "KAB. SANGGAU", "KAB. SEKADAU", "KAB. KAPUAS HULU", "KAB. KUBU RAYA"]

tahuns = ["2023", "2022"]

pilih = st.sidebar.selectbox("Pilih UKPBJ yang diinginkan :", daerah)
tahun = st.sidebar.selectbox("Pilih Tahun :", tahuns)

if pilih == "PROV. KALBAR":
    kodeFolder = "prov"
if pilih == "KAB. BENGKAYANG":
    kodeFolder = "bky"
if pilih == "KAB. MELAWI":
    kodeFolder = "mlw"
if pilih == "KOTA PONTIANAK":
    kodeFolder = "ptk"
if pilih == "KAB. SANGGAU":
    kodeFolder = "sgu"
if pilih == "KAB. SEKADAU":
    kodeFolder = "skd"
if pilih == "KAB. KAPUAS HULU":
    kodeFolder = "kph"
if pilih == "KAB. KUBU RAYA":
    kodeFolder = "kkr"

# Persiapan Dataset
con = duckdb.connect(database=':memory:')

## Akses file dataset format parquet dari Google Cloud Storage via URL Public

### Dataset SPSE Tender
DatasetSPSETenderPengumuman = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderPengumuman{tahun}.parquet"
DatasetSPSETenderSelesai = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderSelesai{tahun}.parquet"
DatasetSPSETenderSelesaiNilai = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderSelesaiNilai{tahun}.parquet"
DatasetSPSETenderSPPBJ = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderEkontrakSPPBJ{tahun}.parquet"
DatasetSPSETenderKontrak = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderEkontrakKontrak{tahun}.parquet"
DatasetSPSETenderSPMK = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderEkontrakSPMKSPP{tahun}.parquet"
DatasetSPSETenderBAST = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSETenderEkontrakBAPBAST{tahun}.parquet"

### Dataset SPSE Non Tender
DatasetSPSENonTenderPengumuman = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSENonTenderPengumuman{tahun}.parquet"
DatasetSPSENonTenderSelesai = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSENonTenderSelesai{tahun}.parquet"
DatasetSPSENonTenderSPPBJ = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSENonTenderEkontrakSPPBJ{tahun}.parquet"
DatasetSPSENonTenderKontrak = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSENonTenderEkontrakKontrak{tahun}.parquet"
DatasetSPSENonTenderSPMK = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSENonTenderEkontrakSPMKSPP{tahun}.parquet"
DatasetSPSENonTenderBAST = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSENonTenderEkontrakBAPBAST{tahun}.parquet"

### Dataset Pencatatan
DatasetCatatNonTender = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSEPencatatanNonTender{tahun}.parquet"
DatasetCatatNonTenderRealisasi = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSEPencatatanNonTenderRealisasi{tahun}.parquet"
DatasetCatatSwakelola = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSEPencatatanSwakelola{tahun}.parquet"
DatasetCatatSwakelolaRealisasi = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSEPencatatanSwakelolaRealisasi{tahun}.parquet"

### Dataset Peserta Tender
DatasetPesertaTender = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/spse/SPSEPesertaTender{tahun}.parquet"

## Buat dataframe SPSE
### Baca file parquet dataset SPSE Tender
try:
    df_SPSETenderPengumuman = pd.read_parquet(DatasetSPSETenderPengumuman)
    df_SPSETenderSelesai = pd.read_parquet(DatasetSPSETenderSelesai)
    df_SPSETenderSelesaiNilai = pd.read_parquet(DatasetSPSETenderSelesaiNilai)
    df_SPSETenderSPPBJ = pd.read_parquet(DatasetSPSETenderSPPBJ)
    df_SPSETenderKontrak = pd.read_parquet(DatasetSPSETenderKontrak)
    df_SPSETenderSPMK = pd.read_parquet(DatasetSPSETenderSPMK)
    df_SPSETenderBAST = pd.read_parquet(DatasetSPSETenderBAST)

except Exception:
    st.error("Gagal baca dataset SPSE Tender")

### Baca file parquet dataset SPSE Non Tender
try:
    df_SPSENonTenderPengumuman = pd.read_parquet(DatasetSPSENonTenderPengumuman)
    df_SPSENonTenderSelesai = pd.read_parquet(DatasetSPSENonTenderSelesai)
    df_SPSENonTenderSPPBJ = pd.read_parquet(DatasetSPSENonTenderSPPBJ)
    df_SPSENonTenderKontrak = pd.read_parquet(DatasetSPSENonTenderKontrak)
    df_SPSENonTenderSPMK = pd.read_parquet(DatasetSPSENonTenderSPMK)
    df_SPSENonTenderBAST = pd.read_parquet(DatasetSPSENonTenderBAST)

except Exception:
    st.error("Gagal baca dataset SPSE Non Tender")

### Baca file parquet dataset Pencatatan
try:
    df_CatatNonTender = pd.read_parquet(DatasetCatatNonTender)
    df_CatatNonTenderRealisasi = pd.read_parquet(DatasetCatatNonTenderRealisasi)
    df_CatatSwakelola = pd.read_parquet(DatasetCatatSwakelola)
    df_CatatSwakelolaRealisasi = pd.read_parquet(DatasetCatatSwakelolaRealisasi)

except Exception:
    st.error("Gagal baca dataset Pencatatan")

### Baca file parquet dataset Peserta Tender
try:
    df_PesertaTender = pd.read_parquet(DatasetPesertaTender)

except Exception:
    st.error("Gagal baca dataset Peserta Tender")

#####
# Mulai membuat presentasi data SPSE
#####

# Buat menu yang mau disajikan
menu_spse_1, menu_spse_2, menu_spse_3, menu_spse_4 = st.tabs(["| TENDER |", "| NON TENDER |", "| PENCATATAN |", "| PESERTA TENDER |"])

## Tab menu SPSE - Tender
with menu_spse_1:

    st.header("SPSE - Tender")

    ### Buat sub menu SPSE - Tender
    menu_spse_1_1, menu_spse_1_2, menu_spse_1_3, menu_spse_1_4, menu_spse_1_5, menu_spse_1_6 = st.tabs(["| PENGUMUMAN |", "| SELESAI |", "| SPPBJ |", "| KONTRAK |", "| SPMK |", "| BAPBAST |"])

    #### Tab menu SPSE - Tender - Pengumuman
    with menu_spse_1_1:

        st.subheader("SPSE-Tender-Pengumuman")

    #### Tab menu SPSE - Tender - Selesai
    with menu_spse_1_2:
        
        st.subheader("SPSE-Tender-Pengumuman")

    #### Tab menu SPSE - Tender - SPPBJ
    with menu_spse_1_3:

        st.subheader("SPSE-Tender-SPPBJ")

    #### Tab menu SPSE - Tender - Kontrak
    with menu_spse_1_4:

        st.subheader("SPSE-Tender-Kontrak")

    #### Tab menu SPSE - Tender - SPMK
    with menu_spse_1_5:

        st.subheader("SPSE-Tender-SPMK")

    #### Tab menu SPSE - Tender - BAPBAST
    with menu_spse_1_6:

        st.subheader("SPSE-Tender-BAPBAST")

## Tab menu SPSE - Non Tender
with menu_spse_2:

    st.header("SPSE - Non Tender")

## Tab menu SPSE - Pencatatan
with menu_spse_3:

    st.header("SPSE - Pencatatan")

## Tab menu SPSE - Peserta Tender
with menu_spse_4:

    st.header("SPSE - Peserta Tender")
