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
# Import library Google Cloud Storage
from google.oauth2 import service_account
from google.cloud import storage
# Import fungsi pribadi
#from fungsi import *

# Konfigurasi variabel lokasi UKPBJ
daerah =    ["PROV. KALBAR", "KOTA PONTIANAK", "KAB. KUBU RAYA", "KAB. MEMPAWAH", "KOTA SINGKAWANG", "KAB. SAMBAS", 
            "KAB. BENGKAYANG", "KAB. LANDAK", "KAB. SANGGAU", "KAB. SEKADAU", "KAB. SINTANG", "KAB. MELAWI", "KAB. KAPUAS HULU", 
            "KAB. KAYONG UTARA", "KAB. KETAPANG"]

tahuns = [2023, 2022]

pilih = st.sidebar.selectbox("Pilih UKPBJ yang diinginkan :", daerah)
tahun = st.sidebar.selectbox("Pilih Tahun :", tahuns)

if pilih == "KAB. BENGKAYANG":
    kodeFolder = "bky"
elif pilih == "KAB. KAPUAS HULU":
    kodeFolder = "kph"
elif pilih == "KAB. KAYONG UTARA":
    kodeFolder = "kku"
elif pilih == "KAB. KETAPANG":
    kodeFolder = "ktp"
elif pilih == "KAB. KUBU RAYA":
    kodeFolder = "kkr"
elif pilih == "KAB. LANDAK":
    kodeFolder = "ldk"
elif pilih == "KAB. MELAWI":
    kodeFolder = "mlw"
elif pilih == "KAB. MEMPAWAH":
    kodeFolder = "mpw"
elif pilih == "KAB. SAMBAS":
    kodeFolder = "sbs"
elif pilih == "KAB. SANGGAU":
    kodeFolder = "sgu"
elif pilih == "KAB. SEKADAU":
    kodeFolder = "skd"
elif pilih == "KAB. SINTANG":
    kodeFolder = "stg"
elif pilih == "KOTA PONTIANAK":
    kodeFolder = "ptk"
elif pilih == "KOTA SINGKAWANG":
    kodeFolder = "skw"
elif pilih == "PROV. KALBAR":
    kodeFolder = "prov"

## Dataset SIRUP
con = duckdb.connect(database=':memory:')

### File path dan unduh file parquet dan simpan di memory
DatasetSIRUPDP = f"https://storage.googleapis.com/dashukpbj_pub/itkp/{kodeFolder}/sirupdp{str(tahun)}.parquet"
DatasetSIRUPDSW = f"https://storage.googleapis.com/dashukpbj_pub/itkp/{kodeFolder}/sirupdsw{str(tahun)}.parquet"
DatasetSIRUPDSARSAP = f"https://storage.googleapis.com/dashukpbj_pub/itkp/{kodeFolder}/sirupdsa_rsap{str(tahun)}.parquet"

# Unduh data parquet SIRUP
try:
    ## Buat Dataframe SIRUP Data Penyedia
    df_SIRUPDP = pd.read_parquet(DatasetSIRUPDP)

    ## Query Data RUP Paket Penyedia

    ## Query Nama Satker Unik
    namaopd = df_SIRUPDP['namasatker'].unique()

except Exception:
    st.error("Gagal baca Dataset SIRUP Data Penyedia.")

####

# Buat Tab Info Utama UKPBJ dan Perangkat Daerah
tabif1, tabif2 = st.tabs(["DAERAH", "PERANGKAT DAERAH"])

# Tab Daerah
with tabif1:
    st.subheader(f"Dashboard Daerah Tahun Anggaran {tahun}")

    cif11, cif12 = st.columns(2)
    with cif12:
        col1, col2, col3 = st.columns(3)
        col1.metric("SATKER", "86")
        col2.metric("PPK", "62")
        col3.metric("BELANJA PENGADAAN", "1000")

    cif21, cif22 = st.columns(2)
    with cif21:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.error("PERENCANAAN")
            with st.expander("Lihat data"):
                st.metric("PERENCANAAN", "86")
                st.metric("PDN", "62")
                st.metric("UMK", "1000")
                st.metric("Penyedia", "1500")
                st.metric("Swakelola", "2000")
        with col2:
            st.warning("PERSIAPAN")
            st.metric("PERSIAPAN", "86")
            st.metric("PDN", "62")
            st.metric("UMK", "1000")
            st.metric("Penyedia", "1500")
            st.metric("Swakelola", "2000")
        with col3:
            st.success("PEMILIHAN")
            st.metric("PEMILIHAN", "86")
            st.metric("PDN", "62")
            st.metric("UMK", "1000")
            st.metric("Penyedia", "1500")
            st.metric("Swakelola", "2000")
    with cif22:
        col1, col2, col3 = st.columns(3)
        col1.metric("KONTRAK", "86")
        col2.metric("SERAH TERIMA", "62")
        col3.metric("PEMBAYARAN", "1000")

with tabif2:
    st.subheader(f"Dashboard Perangkat Daerah Tahun Anggaran {tahun}")
    opd = st.selectbox("Pilih Perangkat Daerah :", namaopd, key='tabif2')


c1, c2 = st.columns(2)
with c1:
    c1t1, c1t2 = st.tabs(["Pagu", "Paket"])
    with c1t1:
        st.markdown("Pagu")
    with c1t2:
        st.markdown("Paket")
with c2:
    c2t1, c2t2 = st.tabs(["Pagu", "Paket"])
    with c2t1:
        st.markdown("Pagu")
    with c2t2:
        st.markdown("Paket")