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
##from google.oauth2 import service_account
##from google.cloud import storage
# Import fungsi pribadi
from fungsi import *
from streamlit_extras.metric_cards import style_metric_cards

# Konfigurasi variabel lokasi UKPBJ
daerah =    ["PROV. KALBAR"]

tahuns = [2023, 2022]

pilih = st.sidebar.selectbox("Pilih UKPBJ yang diinginkan :", daerah)
tahun = st.sidebar.selectbox("Pilih Tahun :", tahuns)

if pilih == "PROV. KALBAR":
    kodeFolder = "prov"

# Persiapan Dataset
con = duckdb.connect(database=':memory:')

## Akses file dataset format parquet dari Google Cloud Storage via URL public
DatasetRUPPP = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPPaketPenyediaTerumumkan{tahun}.parquet"
DatasetRUPPS = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPPaketSwakelolaTerumumkan{tahun}.parquet"
DatasetRUPSA = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPStrukturAnggaranPD{tahun}.parquet"

## Buat dataframe RUP
try:
    ### Baca file parquet dataset RUP Paket Penyedia
    df_RUPPP = pd.read_parquet(DatasetRUPPP)

    ### Query RUP Paket Penyedia
    df_RUPPP_umumkan = con.execute("SELECT * FROM df_RUPPP WHERE status_umumkan_rup = 'Terumumkan'").df()
    df_RUPPP_belum_umumkan = con.execute("SELECT * FROM df_RUPPP WHERE status_umumkan_rup = 'Terinisiasi'").df()
    df_RUPPP_umumkan_ukm = con.execute("SELECT * FROM df_RUPPP_umumkan WHERE status_ukm = 'UKM'").df()
    df_RUPPP_umumkan_pdn = con.execute("SELECT * FROM df_RUPPP_umumkan WHERE status_pdn = 'PDN'").df()

    namaopd = df_RUPPP_umumkan['nama_satker'].unique()

except Exception:
    st.error("Gagal baca dataset RUP Paket Penyedia.")

try:
    ### Baca file parquet dataset RUP Paket Swakelola
    df_RUPPS = pd.read_parquet(DatasetRUPPS)

    ### Query RUP Paket Swakelola
    df_RUPPS_umumkan = con.execute("SELECT * FROM df_RUPPS WHERE status_umumkan_rup = 'Terumumkan'").df()

except Exception:
    st.error("Gagal baca dataset RUP Paket Swakelola.")

try:
    ### Baca file parquet dataset RUP Struktur Anggaran
    df_RUPSA = pd.read_parquet(DatasetRUPSA)

except Exception:
    st.error("Gagal baca dataset RUP Struktur Anggaran.")

#####
# Mulai membuat presentasi data RUP
#####

# Buat menu yang mau disajikan
menurup1, menurup2, menurup3, menurup4, menurup5, menurup6 = st.tabs(["| STRUKTUR ANGGARAN |", "| PROFIL RUP DAERAH |", "| PROFIL RUP PERANGKAT DAERAH |", "| % INPUT RUP |", "| TABEL RUP PAKET PENYEDIA |", "| TABEL RUP PAKET SWAKELOLA |"])

## Tab menu STRUKTUR ANGGARAN
with menurup1:

    st.header(f"STRUKTUR ANGGARAN {pilih} TAHUN {tahun}", divider='rainbow')

    sql_query_sa = """
        SELECT nama_satker AS NAMA_SATKER, SUM(belanja_operasi) AS BELANJA_OPERASI, SUM(belanja_modal) AS BELANJA_MODAL, SUM(belanja_pengadaan) AS BELANJA_PENGADAAN, SUM(total_belanja) AS TOTAL_BELANJA
        FROM df_RUPSA
        GROUP BY nama_satker
        ORDER BY total_belanja DESC;
    """

    df_RUPSA_tampil = con.execute(sql_query_sa).df()

    ### Tampilkan data menggunakan AgGrid
    gd = GridOptionsBuilder.from_dataframe(df_RUPSA_tampil)
    gd.configure_pagination()
    gd.configure_side_bar()
    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    gd.configure_column("BELANJA_OPERASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.BELANJA_OPERASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("BELANJA_MODAL", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.BELANJA_MODAL.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("BELANJA_PENGADAAN", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.BELANJA_PENGADAAN.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("TOTAL_BELANJA", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.TOTAL_BELANJA.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

    gridOptions = gd.build()
    AgGrid(df_RUPSA_tampil, gridOptions=gridOptions, enable_enterprise_modules=True)

## Tab menu PROFIL RUP DAERAH
with menurup2:

    ### Hitung-hitung dataset
    df_RUPPP_mp_hitung = con.execute("SELECT metode_pengadaan AS METODE_PENGADAAN, COUNT(metode_pengadaan) AS JUMLAH_PAKET FROM df_RUPPP_umumkan WHERE metode_pengadaan IS NOT NULL GROUP BY metode_pengadaan").df() 
    df_RUPPP_mp_nilai = con.execute("SELECT metode_pengadaan AS METODE_PENGADAAN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_umumkan WHERE metode_pengadaan IS NOT NULL GROUP BY metode_pengadaan").df()
    df_RUPPP_jp_hitung = con.execute("SELECT jenis_pengadaan AS JENIS_PENGADAAN, COUNT(jenis_pengadaan) AS JUMLAH_PAKET FROM df_RUPPP_umumkan WHERE jenis_pengadaan IS NOT NULL GROUP BY jenis_pengadaan").df()
    df_RUPPP_jp_nilai = con.execute("SELECT jenis_pengadaan AS JENIS_PENGADAAN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_umumkan WHERE jenis_pengadaan IS NOT NULL GROUP BY Jenis_pengadaan").df()

    ### Buat tombol unduh dataset
    unduh_RUPPP = unduh_data(df_RUPPP_umumkan)
    unduh_RUPSW = unduh_data(df_RUPPS_umumkan)

    prd1, prd2, prd3 = st.columns((6,2,2))
    with prd1:
        st.header(f"PROFIL RUP {pilih} TAHUN {tahun}")
    with prd2:
        st.download_button(
            label = "📥 Download RUP Paket Penyedia",
            data = unduh_RUPPP,
            file_name = f"RUPPaketPenyedia-{kodeFolder}.csv",
            mime = "text/csv"
        )
    with prd3:
        st.download_button(
            label = "📥 Download RUP Paket Swakelola",
            data = unduh_RUPSW,
            file_name = f"RUPPaketSwakelola-{kodeFolder}.csv",
            mime = "text/csv"
        )

    st.divider()

    st.subheader("STRUKTUR ANGGARAN")

    belanja_pengadaan = df_RUPSA['belanja_pengadaan'].sum()
    belanja_operasional = df_RUPSA['belanja_operasi'].sum()
    belanja_modal = df_RUPSA['belanja_modal'].sum()
    belanja_total = belanja_pengadaan + belanja_operasional + belanja_modal

    colsa11, colsa12, colsa13, colsa14 = st.columns(4)
    colsa11.metric(label="Belanja Pengadaan", value="{:,.2f}".format(belanja_pengadaan))
    colsa12.metric(label="Belanja Operasional", value="{:,.2f}".format(belanja_operasional))
    colsa13.metric(label="Belanja Modal", value="{:,.2f}".format(belanja_modal))
    colsa14.metric(label="Belanja Total", value="{:,.2f}".format(belanja_total))  
    style_metric_cards()  
    
    st.divider()

    st.subheader("POSISI INPUT RUP")

    jumlah_total_rup = df_RUPPP_umumkan.shape[0] + df_RUPPS_umumkan.shape[0]
    nilai_total_rup = df_RUPPP_umumkan['pagu'].sum() + df_RUPPS_umumkan['pagu'].sum()
    persen_capaian_rup = nilai_total_rup / belanja_pengadaan

    colir11, colir12, colir13 = st.columns(3)
    colir11.subheader("Jumlah Total")
    colir12.metric(label="Jumlah Total Paket RUP", value="{:,.2f}".format(jumlah_total_rup))
    colir13.metric(label="Nilai Total Paket RUP", value="{:,.2f}".format(nilai_total_rup))
    style_metric_cards()
    colir21, colir22, colir23 = st.columns(3)
    colir21.subheader("Paket Penyedia")
    colir22.metric(label="Jumlah Total Paket Penyedia", value="{:,.2f}".format(df_RUPPP_umumkan.shape[0]))
    colir23.metric(label="Nilai Total Paket Penyedia", value="{:,.2f}".format(df_RUPPP_umumkan['pagu'].sum()))
    style_metric_cards()
    colir31, colir32, colir33 = st.columns(3)
    colir31.subheader("Paket Swakelola")
    colir32.metric(label="Jumlah Total Paket Swakelola", value="{:,.2f}".format(df_RUPPS_umumkan.shape[0]))
    colir33.metric(label="Nilai Total Paket Swakelola", value="{:,.2f}".format(df_RUPPS_umumkan['pagu'].sum()))
    style_metric_cards()
    colir41, colir42, colir43 = st.columns(3)
    colir41.subheader("")
    colir42.subheader("")
    colir43.metric(label="Persentase Capaian RUP", value="{:.2%}".format(persen_capaian_rup))
    style_metric_cards()

    st.divider()

    st.subheader("BERDASARKAN METODE PENGADAAN")

    mph1, mph2 = st.columns((5,5))
    with mph1:
        st.markdown("#### Berdasarkan Jumlah Paket")
        AgGrid(df_RUPPP_mp_hitung)

        st.divider()

    with mph2:
        st.markdown("#### Berdasarkan Nilai Paket")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_mp_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_mp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

        st.divider()



