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

    cif11, cif12 = st.columns((5,5))
    with cif12:
        st.markdown("Data Satker, PPK dan Belanja Pengadaan")

with tabif2:
    st.subheader(f"Dashboard Perangkat Daerah Tahun Anggaran {tahun}")
    opd = st.selectbox("Pilih Perangkat Daerah :", namaopd, key='tabif2')


c1, c2 = st.columns((5,5))
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