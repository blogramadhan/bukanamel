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
from streamlit_extras.metric_cards import style_metric_cards
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
    df_SPSETenderPengumuman = tarik_data(DatasetSPSETenderPengumuman)
    df_SPSETenderSelesai = tarik_data(DatasetSPSETenderSelesai)
    df_SPSETenderSelesaiNilai = tarik_data(DatasetSPSETenderSelesaiNilai)
    df_SPSETenderSPPBJ = tarik_data(DatasetSPSETenderSPPBJ)
    df_SPSETenderKontrak = tarik_data(DatasetSPSETenderKontrak)
    df_SPSETenderSPMK = tarik_data(DatasetSPSETenderSPMK)
    df_SPSETenderBAST = tarik_data(DatasetSPSETenderBAST)

except Exception:
    st.error("Gagal baca dataset SPSE Tender")

### Baca file parquet dataset SPSE Non Tender
try:
    df_SPSENonTenderPengumuman = tarik_data(DatasetSPSENonTenderPengumuman)
    df_SPSENonTenderSelesai = tarik_data(DatasetSPSENonTenderSelesai)
    df_SPSENonTenderSPPBJ = tarik_data(DatasetSPSENonTenderSPPBJ)
    df_SPSENonTenderKontrak = tarik_data(DatasetSPSENonTenderKontrak)
    df_SPSENonTenderSPMK = tarik_data(DatasetSPSENonTenderSPMK)
    df_SPSENonTenderBAST = tarik_data(DatasetSPSENonTenderBAST)

except Exception:
    st.error("Gagal baca dataset SPSE Non Tender")

### Baca file parquet dataset Pencatatan
try:
    df_CatatNonTender = tarik_data(DatasetCatatNonTender)
    df_CatatNonTenderRealisasi = tarik_data(DatasetCatatNonTenderRealisasi)
    df_CatatSwakelola = tarik_data(DatasetCatatSwakelola)
    df_CatatSwakelolaRealisasi = tarik_data(DatasetCatatSwakelolaRealisasi)

except Exception:
    st.error("Gagal baca dataset Pencatatan")

### Baca file parquet dataset Peserta Tender
try:
    df_PesertaTender = tarik_data(DatasetPesertaTender)

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

        ##### Buat tombol unduh dataset
        unduh_SPSE_Pengumuman = unduh_data(df_SPSETenderPengumuman)
        
        SPSE_Umumkan_1, SPSE_Umumkan_2 = st.columns((7,3))
        with SPSE_Umumkan_1:
            st.subheader("Pengumuman Tender")
        with SPSE_Umumkan_2:
            st.download_button(
                label = "📥 Download Data Pengumuman Tender",
                data = unduh_SPSE_Pengumuman,
                file_name = f"SPSETenderPengumuman-{kodeFolder}-{tahun}.csv",
                mime = "text/csv"
            )

        st.divider()

        SPSE_radio_1, SPSE_radio_2, SPSE_radio_3 = st.columns((1,1,8))
        with SPSE_radio_1:
            sumber_dana = st.radio("**Sumber Dana**", ["APBD", "APBDP", "BLUD"])
        with SPSE_radio_2:
            status_tender = st.radio("**Status Tender**", ["Selesai", "Gagal/Batal", "Berlangsung"])
        st.write(f"Anda memilih : **{sumber_dana}** dan **{status_tender}**")

        ##### Hitung-hitungan dataset
        df_SPSETenderPengumuman_filter = con.execute(f"SELECT kd_tender, pagu, hps, kualifikasi_paket FROM df_SPSETenderPengumuman WHERE sumber_dana = '{sumber_dana}' AND status_tender = '{status_tender}'").df()
        jumlah_trx_spse_pengumuman = df_SPSETenderPengumuman_filter['kd_tender'].unique().shape[0]
        nilai_trx_spse_pengumuman_pagu = df_SPSETenderPengumuman_filter['pagu'].sum()
        nilai_trx_spse_pengumuman_hps = df_SPSETenderPengumuman_filter['hps'].sum()

        menu_trx_1, menu_trx_2, menu_trx_3 = st.columns(3)
        menu_trx_1.metric(label="Jumlah Tender Diumumkan", value="{:,}".format(jumlah_trx_spse_pengumuman))
        menu_trx_2.metric(label="Nilai Pagu Tender Diumumkan", value="{:,.2f}".format(nilai_trx_spse_pengumuman_pagu))
        menu_trx_3.metric(label="Nilai HPS Tender Diumumkan", value="{:,.2f}".format(nilai_trx_spse_pengumuman_hps))
        style_metric_cards()

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan kualifikasi paket
        grafik_kp_1, grafik_kp_2 = st.tabs(["| Berdasarkan Jumlah Kualifikasi Paket |", "| Berdasarkan Nilai Kualifikasi Paket |"])

        with grafik_kp_1:

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan kualifikasi paket

            sql_kp_jumlah = """
                SELECT kualifikasi_paket AS KUALIFIKASI_PAKET, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY KUALIFIKASI_PAKET ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_kp_jumlah_trx = con.execute(sql_kp_jumlah).df()

            grafik_kp_1_1, grafik_kp_1_2 = st.columns((4,6))

            with grafik_kp_1_1:

                AgGrid(tabel_kp_jumlah_trx)

            with grafik_kp_1_2:

                grafik_kp_jumlah_trx = px.bar(tabel_kp_jumlah_trx, x='KUALIFIKASI_PAKET', y='JUMLAH_PAKET', text_auto='.2s', title='Grafik Jumlah Tender di Umumkan Berdasarkan Kualifikasi Paket')
                grafik_kp_jumlah_trx.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                st.plotly_chart(grafik_kp_jumlah_trx, theme='streamlit', use_container_width=True) 

        with grafik_kp_2:

            st.subheader("Berdasarkan Nilai Kualifikasi Paket")


    #### Tab menu SPSE - Tender - Selesai
    with menu_spse_1_2:
        
        st.subheader("SPSE-Tender-Selesai")

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
