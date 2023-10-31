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

# Konfigurasi variabel lokasi UKPBJ
daerah =    ["PROV. KALBAR", "KAB. BENGKAYANG", "KAB. MELAWI", "KOTA PONTIANAK", "KAB. SANGGAU", "KAB. SEKADAU", "KAB. KAPUAS HULU", "KAB. KUBU RAYA"]

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

## Akses file dataset format parquet dari Google Cloud Storage via URL public
DatasetRUPPP = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPPaketPenyediaTerumumkan{tahun}.parquet"
DatasetRUPPS = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPPaketSwakelolaTerumumkan{tahun}.parquet"
DatasetRUPSA = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPStrukturAnggaran{tahun}.parquet"

## Buat dataframe RUP
try:
    ### Baca file parquet dataset RUP Paket Penyedia
    df_RUPPP = tarik_data(DatasetRUPPP)

    ### Query RUP Paket Penyedia
    df_RUPPP_umumkan = con.execute("SELECT * FROM df_RUPPP WHERE status_umumkan_rup = 'Terumumkan' AND status_aktif_rup = 'TRUE'").df()
    df_RUPPP_belum_umumkan = con.execute("SELECT * FROM df_RUPPP WHERE status_umumkan_rup = 'Terinisiasi'").df()
    df_RUPPP_umumkan_ukm = con.execute("SELECT * FROM df_RUPPP_umumkan WHERE status_ukm = 'UKM'").df()
    df_RUPPP_umumkan_pdn = con.execute("SELECT * FROM df_RUPPP_umumkan WHERE status_pdn = 'PDN'").df()

    namaopd = df_RUPPP_umumkan['nama_satker'].unique()

except Exception:
    st.error("Gagal baca dataset RUP Paket Penyedia.")

try:
    ### Baca file parquet dataset RUP Paket Swakelola
    df_RUPPS = tarik_data(DatasetRUPPS)

    ### Query RUP Paket Swakelola
    df_RUPPS_umumkan = con.execute("SELECT * FROM df_RUPPS WHERE status_umumkan_rup = 'Terumumkan'").df()

except Exception:
    st.error("Gagal baca dataset RUP Paket Swakelola.")

try:
    ### Baca file parquet dataset RUP Struktur Anggaran
    df_RUPSA = tarik_data(DatasetRUPSA)

except Exception:
    st.error("Gagal baca dataset RUP Struktur Anggaran.")

#####
# Mulai membuat presentasi data RUP
#####

# Buat menu yang mau disajikan
menu_rup_1, menu_rup_2, menu_rup_3, menu_rup_4, menu_rup_5, menu_rup_6 = st.tabs(["| PROFIL RUP DAERAH |", "| PROFIL RUP PERANGKAT DAERAH |", "| STRUKTUR ANGGARAN |", "| % INPUT RUP |", "| TABEL RUP PAKET PENYEDIA |", "| TABEL RUP PAKET SWAKELOLA |"])

## Tab menu PROFIL RUP DAERAH
with menu_rup_1:

    ### Hitung-hitung dataset
    df_RUPPP_mp_hitung = con.execute("SELECT metode_pengadaan AS METODE_PENGADAAN, COUNT(metode_pengadaan) AS JUMLAH_PAKET FROM df_RUPPP_umumkan WHERE metode_pengadaan IS NOT NULL GROUP BY metode_pengadaan").df() 
    df_RUPPP_mp_nilai = con.execute("SELECT metode_pengadaan AS METODE_PENGADAAN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_umumkan WHERE metode_pengadaan IS NOT NULL GROUP BY metode_pengadaan").df()
    df_RUPPP_jp_hitung = con.execute("SELECT jenis_pengadaan AS JENIS_PENGADAAN, COUNT(jenis_pengadaan) AS JUMLAH_PAKET FROM df_RUPPP_umumkan WHERE jenis_pengadaan IS NOT NULL GROUP BY jenis_pengadaan").df()
    df_RUPPP_jp_nilai = con.execute("SELECT jenis_pengadaan AS JENIS_PENGADAAN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_umumkan WHERE jenis_pengadaan IS NOT NULL GROUP BY Jenis_pengadaan").df()
    df_RUPPP_ukm_hitung = con.execute("SELECT status_ukm AS STATUS_UKM, COUNT(status_ukm) AS JUMLAH_PAKET FROM df_RUPPP_umumkan WHERE status_ukm IS NOT NULL GROUP BY status_ukm").df()
    df_RUPPP_ukm_nilai = con.execute("SELECT status_ukm AS STATUS_UKM, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_umumkan WHERE status_ukm IS NOT NULL GROUP BY status_ukm").df()
    df_RUPPP_pdn_hitung = con.execute("SELECT status_pdn AS STATUS_PDN, COUNT(status_pdn) AS JUMLAH_PAKET FROM df_RUPPP_umumkan WHERE status_pdn IS NOT NULL GROUP BY status_pdn").df()
    df_RUPPP_pdn_nilai = con.execute("SELECT status_pdn AS STATUS_PDN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_umumkan WHERE status_pdn IS NOT NULL GROUP BY status_pdn").df() 

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
    colir12.metric(label="Jumlah Total Paket RUP", value="{:,}".format(jumlah_total_rup))
    colir13.metric(label="Nilai Total Paket RUP", value="{:,.2f}".format(nilai_total_rup))
    style_metric_cards()
    colir21, colir22, colir23 = st.columns(3)
    colir21.subheader("Paket Penyedia")
    colir22.metric(label="Jumlah Total Paket Penyedia", value="{:,}".format(df_RUPPP_umumkan.shape[0]))
    colir23.metric(label="Nilai Total Paket Penyedia", value="{:,.2f}".format(df_RUPPP_umumkan['pagu'].sum()))
    style_metric_cards()
    colir31, colir32, colir33 = st.columns(3)
    colir31.subheader("Paket Swakelola")
    colir32.metric(label="Jumlah Total Paket Swakelola", value="{:,}".format(df_RUPPS_umumkan.shape[0]))
    colir33.metric(label="Nilai Total Paket Swakelola", value="{:,.2f}".format(df_RUPPS_umumkan['pagu'].sum()))
    style_metric_cards()
    colir41, colir42, colir43 = st.columns(3)
    colir41.subheader("")
    colir42.subheader("")
    colir43.metric(label="Persentase Capaian RUP", value="{:.2%}".format(persen_capaian_rup))
    style_metric_cards()

    st.divider()

    st.subheader("STATUS UKM DAN PDN")

    ### Buat grafik RUP Status UKM
    grafik_rup_ukm_tab_1, grafik_rup_ukm_tab_2 = st.tabs(["| Berdasarkan Jumlah Paket - UKM |", "| Berdasarkan Nilai Paket - UKM |"])

    with grafik_rup_ukm_tab_1:

        grafik_rup_ukm_tab_1_1, grafik_rup_ukm_tab_1_2 = st.columns((3,7))

        with grafik_rup_ukm_tab_1_1:

            AgGrid(df_RUPPP_ukm_hitung)

        with grafik_rup_ukm_tab_1_2:

            figukmh = px.pie(df_RUPPP_ukm_hitung, values='JUMLAH_PAKET', names='STATUS_UKM', title='Grafik Status UKM - Jumlah Paket', hole=.3)
            st.plotly_chart(figukmh, theme="streamlit", use_container_width=True)

    with grafik_rup_ukm_tab_2:

        grafik_rup_ukm_tab_2_1, grafik_rup_ukm_tab_2_2 = st.columns((3,7))

        with grafik_rup_ukm_tab_2_1:

            gd = GridOptionsBuilder.from_dataframe(df_RUPPP_ukm_nilai)
            gd.configure_pagination()
            gd.configure_side_bar()
            gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

            gridOptions = gd.build()
            AgGrid(df_RUPPP_ukm_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

        with grafik_rup_ukm_tab_2_2:

            figukmn = px.pie(df_RUPPP_ukm_nilai, values='NILAI_PAKET', names='STATUS_UKM', title='Grafik Status UKM - Nilai Paket', hole=.3)
            st.plotly_chart(figukmn, theme='streamlit', use_container_width=True)

    ### Buat grafik RUP Status PDN
    grafik_rup_pdn_tab_1, grafik_rup_pdn_tab_2 = st.tabs(["| Berdasarkan Jumlah Paket - PDN |", "| Berdasarkan Nilai Paket - PDN |"])

    with grafik_rup_pdn_tab_1:

        grafik_rup_pdn_tab_1_1, grafik_rup_pdn_tab_1_2 = st.columns((3,7))

        with grafik_rup_pdn_tab_1_1:

            AgGrid(df_RUPPP_pdn_hitung)

        with grafik_rup_pdn_tab_1_2:

            figpdnh = px.pie(df_RUPPP_pdn_hitung, values='JUMLAH_PAKET', names='STATUS_PDN', title='Grafik Status PDN - Jumlah Paket', hole=.3)
            st.plotly_chart(figpdnh, theme="streamlit", use_container_width=True)

    with grafik_rup_pdn_tab_2:

        grafik_rup_pdn_tab_2_1, grafik_rup_pdn_tab_2_2 = st.columns((3,7))

        with grafik_rup_pdn_tab_2_1:

            gd = GridOptionsBuilder.from_dataframe(df_RUPPP_pdn_nilai)
            gd.configure_pagination()
            gd.configure_side_bar()
            gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

            gridOptions = gd.build()
            AgGrid(df_RUPPP_pdn_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

        with grafik_rup_pdn_tab_2_2:

            figpdnn = px.pie(df_RUPPP_pdn_nilai, values='NILAI_PAKET', names='STATUS_PDN', title='Grafik Status PDN - Nilai Paket', hole=.3)
            st.plotly_chart(figpdnn, theme='streamlit', use_container_width=True)

    st.divider()

    st.subheader("BERDASARKAN METODE PENGADAAN")

    mph1, mpn2 = st.columns((5,5))
    with mph1:
        st.markdown("#### Berdasarkan Jumlah Paket")
        AgGrid(df_RUPPP_mp_hitung)

    with mpn2:
        st.markdown("#### Berdasarkan Nilai Paket")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_mp_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_mp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

    mphg1, mpng2 = st.columns((5,5))
    with mphg1:
        figmph = px.pie(df_RUPPP_mp_hitung, values='JUMLAH_PAKET', names='METODE_PENGADAAN', title='Grafik Metode Pengadaan - Jumlah Paket', hole=.3)
        st.plotly_chart(figmph, theme="streamlit", use_container_width=True)
    with mpng2:
        figmpn = px.pie(df_RUPPP_mp_nilai, values='NILAI_PAKET', names='METODE_PENGADAAN', title='Grafik Metode Pengadaan - Nilai Paket', hole=.3)
        st.plotly_chart(figmpn, theme='streamlit', use_container_width=True)

    st.divider()

    st.subheader("BERDASARKAN JENIS PENGADAAN")

    jph1, jpn2 = st.columns((5,5))
    with jph1:
        st.markdown("#### Berdasarkan Jumlah Paket")
        AgGrid(df_RUPPP_jp_hitung)

    with jpn2:
        st.markdown("#### Berdasarkan Nilai Paket")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_jp_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_jp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

    jphg1, jpng2 = st.columns((5,5))
    with jphg1:
        figjph = px.pie(df_RUPPP_jp_hitung, values='JUMLAH_PAKET', names='JENIS_PENGADAAN', title='Grafik Jenis Pengadaan - Jumlah Paket', hole=.3)
        st.plotly_chart(figjph, theme="streamlit", use_container_width=True)
    with jpng2:
        figjpn = px.pie(df_RUPPP_jp_nilai, values='NILAI_PAKET', names='JENIS_PENGADAAN', title='Grafik Jenis Pengadaan - Nilai Paket', hole=.3)
        st.plotly_chart(figjpn, theme='streamlit', use_container_width=True)

## Tab menu PROFIL RUP PERANGKAT DAERAH
with menu_rup_2:

    st.header(f"PROFIL RUP {pilih} PERANGKAT DAERAH TAHUN {tahun}")

    ### Tampilan pilihan menu nama opd
    opd = st.selectbox("Pilih Perangkat Daerah :", namaopd, key='menu_rup_3')

    df_RUPPP_PD = con.execute(f"SELECT * FROM df_RUPPP_umumkan WHERE nama_satker = '{opd}'").df()
    df_RUPPS_PD = con.execute(f"SELECT * FROM df_RUPPS_umumkan WHERE nama_satker = '{opd}'").df()
    df_RUPSA_PD = con.execute(f"SELECT * FROM df_RUPSA WHERE nama_satker = '{opd}'").df()

    ### Hitung-hitung dataset (Perangkat Daerah)
    df_RUPPP_PD_mp_hitung = con.execute("SELECT metode_pengadaan AS METODE_PENGADAAN, COUNT(metode_pengadaan) AS JUMLAH_PAKET FROM df_RUPPP_PD WHERE metode_pengadaan IS NOT NULL GROUP BY metode_pengadaan").df()
    df_RUPPP_PD_mp_nilai = con.execute("SELECT metode_pengadaan AS METODE_PENGADAAN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_PD WHERE metode_pengadaan IS NOT NULL GROUP BY metode_pengadaan").df()
    df_RUPPP_PD_jp_hitung = con.execute("SELECT jenis_pengadaan AS JENIS_PENGADAAN, COUNT(jenis_pengadaan) AS JUMLAH_PAKET FROM df_RUPPP_PD WHERE jenis_pengadaan IS NOT NULL GROUP BY jenis_pengadaan").df()
    df_RUPPP_PD_jp_nilai = con.execute("SELECT jenis_pengadaan AS JENIS_PENGADAAN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_PD WHERE jenis_pengadaan IS NOT NULL GROUP BY Jenis_pengadaan").df()
    df_RUPPP_PD_ukm_hitung = con.execute("SELECT status_ukm AS STATUS_UKM, COUNT(status_ukm) AS JUMLAH_PAKET FROM df_RUPPP_PD WHERE status_ukm IS NOT NULL GROUP BY status_ukm").df()
    df_RUPPP_PD_ukm_nilai = con.execute("SELECT status_ukm AS STATUS_UKM, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_PD WHERE status_ukm IS NOT NULL GROUP BY status_ukm").df()
    df_RUPPP_PD_pdn_hitung = con.execute("SELECT status_pdn AS STATUS_PDN, COUNT(status_pdn) AS JUMLAH_PAKET FROM df_RUPPP_PD WHERE status_pdn IS NOT NULL GROUP BY status_pdn").df()
    df_RUPPP_PD_pdn_nilai = con.execute("SELECT status_pdn AS STATUS_PDN, SUM(pagu) AS NILAI_PAKET FROM df_RUPPP_PD WHERE status_pdn IS NOT NULL GROUP BY status_pdn").df()

    ### Buat tombol unduh dataset PerangKat Daerah
    unduh_RUPPP_PD = unduh_data(df_RUPPP_PD)
    unduh_RUPPS_PD = unduh_data(df_RUPPS_PD)

    prpd1, prpd2, prpd3 = st.columns((6,2,2))
    with prpd1:
        st.subheader(f"{opd}")
    with prpd2:
        st.download_button(
            label = "📥 Download RUP Paket Penyedia",
            data = unduh_RUPPP_PD,
            file_name = f"RUPPaketPenyedia-PD-{kodeFolder}.csv",
            mime = "text/csv"
        )
    with prpd3:
        st.download_button(
            label = "📥 Download RUP Paket Swakelola",
            data = unduh_RUPPS_PD,
            file_name = f"RUPPaketSwakelola-PD-{kodeFolder}.csv",
            mime = "text/csv"
        )

    st.divider()

    st.subheader("STRUKTUR ANGGARAN")

    belanja_pengadaan_pd = df_RUPSA_PD['belanja_pengadaan'].sum()
    belanja_operasional_pd = df_RUPSA_PD['belanja_operasi'].sum()
    belanja_modal_pd = df_RUPSA_PD['belanja_modal'].sum()
    belanja_total_pd = belanja_pengadaan_pd + belanja_operasional_pd + belanja_modal_pd

    colsapd11, colsapd12, colsapd13, colsapd14 = st.columns(4)
    colsapd11.metric(label="Belanja Pengadaan", value="{:,.2f}".format(belanja_pengadaan_pd))
    colsapd12.metric(label="Belanja Operasional", value="{:,.2f}".format(belanja_operasional_pd))
    colsapd13.metric(label="Belanja Modal", value="{:,.2f}".format(belanja_modal_pd))
    colsapd14.metric(label="Belanja Total", value="{:,.2f}".format(belanja_total_pd))  
    style_metric_cards()  

    st.divider()

    st.subheader("POSISI INPUT RUP")

    jumlah_total_rup_pd = df_RUPPP_PD.shape[0] + df_RUPPS_PD.shape[0]
    nilai_total_rup_pd = df_RUPPP_PD['pagu'].sum() + df_RUPPS_PD['pagu'].sum()
    persen_capaian_rup_pd = nilai_total_rup_pd / belanja_pengadaan_pd

    colirpd11, colirpd12, colirpd13 = st.columns(3)
    colirpd11.subheader("Jumlah Total")
    colirpd12.metric(label="Jumlah Total Paket RUP", value="{:,}".format(jumlah_total_rup_pd))
    colirpd13.metric(label="Nilai Total Paket RUP", value="{:,.2f}".format(nilai_total_rup_pd))
    style_metric_cards()
    colirpd21, colirpd22, colirpd23 = st.columns(3)
    colirpd21.subheader("Paket Penyedia")
    colirpd22.metric(label="Jumlah Total Paket Penyedia", value="{:,}".format(df_RUPPP_PD.shape[0]))
    colirpd23.metric(label="Nilai Total Paket Penyedia", value="{:,.2f}".format(df_RUPPP_PD['pagu'].sum()))
    style_metric_cards()
    colirpd31, colirpd32, colirpd33 = st.columns(3)
    colirpd31.subheader("Paket Swakelola")
    colirpd32.metric(label="Jumlah Total Paket Swakelola", value="{:,}".format(df_RUPPS_PD.shape[0]))
    colirpd33.metric(label="Nilai Total Paket Swakelola", value="{:,.2f}".format(df_RUPPS_PD['pagu'].sum()))
    style_metric_cards()
    colirpd41, colirpd42, colirpd43 = st.columns(3)
    colirpd41.subheader("")
    colirpd42.subheader("")
    colirpd43.metric(label="Persentase Capaian RUP", value="{:.2%}".format(persen_capaian_rup_pd))
    style_metric_cards()

    st.divider()

    st.subheader("STATUS UKM DAN PDN")

    ukmhpd1, ukmnpd2 = st.columns((5,5))
    with ukmhpd1:
        st.markdown("#### Berdasarkan Jumlah Paket - UKM")
        AgGrid(df_RUPPP_PD_ukm_hitung)

    with ukmnpd2:
        st.markdown("#### Berdasarkan Nilai Paket - UKM")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_PD_ukm_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_PD_ukm_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

    ukmhgpd1, ukmngpd2 = st.columns((5,5))
    with ukmhgpd1:
        figukmhpd = px.pie(df_RUPPP_PD_ukm_hitung, values='JUMLAH_PAKET', names='STATUS_UKM', title='Grafik Status UKM - Jumlah Paket', hole=.3)
        st.plotly_chart(figukmhpd, theme="streamlit", use_container_width=True)
    with ukmngpd2:
        figukmnpd = px.pie(df_RUPPP_PD_ukm_nilai, values='NILAI_PAKET', names='STATUS_UKM', title='Grafik Status UKM - Nilai Paket', hole=.3)
        st.plotly_chart(figukmnpd, theme='streamlit', use_container_width=True)

    pdnhpd1, pdnnpd2 = st.columns((5,5))
    with pdnhpd1:
        st.markdown("#### Berdasarkan Jumlah Paket - PDN")
        AgGrid(df_RUPPP_PD_pdn_hitung)

    with pdnnpd2:
        st.markdown("#### Berdasarkan Nilai Paket - PDN")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_PD_pdn_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_PD_pdn_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

    pdnhgpd1, pdnngpd2 = st.columns((5,5))
    with pdnhgpd1:
        figpdnhpd = px.pie(df_RUPPP_PD_pdn_hitung, values='JUMLAH_PAKET', names='STATUS_PDN', title='Grafik Status PDN - Jumlah Paket', hole=.3)
        st.plotly_chart(figpdnhpd, theme="streamlit", use_container_width=True)
    with pdnngpd2:
        figpdnnpd = px.pie(df_RUPPP_PD_pdn_nilai, values='NILAI_PAKET', names='STATUS_PDN', title='Grafik Status PDN - Nilai Paket', hole=.3)
        st.plotly_chart(figpdnnpd, theme='streamlit', use_container_width=True)

    st.divider()

    st.subheader("BERDASARKAN METODE PENGADAAN")

    mphpd1, mpnpd2 = st.columns((5,5))
    with mphpd1:
        st.markdown("#### Berdasarkan Jumlah Paket")
        AgGrid(df_RUPPP_PD_mp_hitung)

    with mpnpd2:
        st.markdown("#### Berdasarkan Nilai Paket")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_PD_mp_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_PD_mp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

    mphgpd1, mpngpd2 = st.columns((5,5))
    with mphgpd1:
        figmphpd = px.pie(df_RUPPP_PD_mp_hitung, values='JUMLAH_PAKET', names='METODE_PENGADAAN', title='Grafik Metode Pengadaan - Jumlah Paket', hole=.3)
        st.plotly_chart(figmphpd, theme="streamlit", use_container_width=True)
    with mpngpd2:
        figmpnpd = px.pie(df_RUPPP_PD_mp_nilai, values='NILAI_PAKET', names='METODE_PENGADAAN', title='Grafik Metode Pengadaan - Nilai Paket', hole=.3)
        st.plotly_chart(figmpnpd, theme='streamlit', use_container_width=True)

    st.divider()

    st.subheader("BERDASARKAN JENIS PENGADAAN")

    jphpd1, jpnpd2 = st.columns((5,5))
    with jphpd1:
        st.markdown("#### Berdasarkan Jumlah Paket")
        AgGrid(df_RUPPP_PD_jp_hitung)

    with jpnpd2:
        st.markdown("#### Berdasarkan Nilai Paket")
        gd = GridOptionsBuilder.from_dataframe(df_RUPPP_PD_jp_nilai)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

        gridOptions = gd.build()
        AgGrid(df_RUPPP_PD_jp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)

    jphgpd1, jpngpd2 = st.columns((5,5))
    with jphgpd1:
        figjphpd = px.pie(df_RUPPP_PD_jp_hitung, values='JUMLAH_PAKET', names='JENIS_PENGADAAN', title='Grafik Jenis Pengadaan - Jumlah Paket', hole=.3)
        st.plotly_chart(figjphpd, theme="streamlit", use_container_width=True)
    with jpngpd2:
        figjpnpd = px.pie(df_RUPPP_jp_nilai, values='NILAI_PAKET', names='JENIS_PENGADAAN', title='Grafik Jenis Pengadaan - Nilai Paket', hole=.3)
        st.plotly_chart(figjpnpd, theme='streamlit', use_container_width=True)

## Tab menu STRUKTUR ANGGARAN
with menu_rup_3:

    st.header(f"STRUKTUR ANGGARAN {pilih} TAHUN {tahun}", divider='rainbow')

    sql_query_sa = """
        SELECT nama_satker AS NAMA_SATKER, SUM(belanja_operasi) AS BELANJA_OPERASI, SUM(belanja_modal) AS BELANJA_MODAL, SUM(belanja_pengadaan) AS BELANJA_PENGADAAN, SUM(total_belanja) AS TOTAL_BELANJA
        FROM df_RUPSA
        WHERE BELANJA_PENGADAAN > 0
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

## Tab menu % INPUT RUP
with menu_rup_4:

    st.header(f"% INPUT RUP {pilih} TAHUN {tahun}", divider="rainbow")

    ir_strukturanggaran = con.execute("SELECT nama_satker AS NAMA_SATKER, belanja_pengadaan AS STRUKTUR_ANGGARAN FROM df_RUPSA WHERE STRUKTUR_ANGGARAN > 0").df()
    ir_paketpenyedia = con.execute("SELECT nama_satker AS NAMA_SATKER, SUM(pagu) AS RUP_PENYEDIA FROM df_RUPPP_umumkan GROUP BY NAMA_SATKER").df()
    ir_paketswakelola = con.execute("SELECT nama_satker AS NAMA_SATKER, SUM(pagu) AS RUP_SWAKELOLA FROM df_RUPPS_umumkan GROUP BY NAMA_SATKER").df()   

    ir_gabung = pd.merge(pd.merge(ir_strukturanggaran, ir_paketpenyedia, on='NAMA_SATKER'), ir_paketswakelola, on='NAMA_SATKER')
    ir_gabung_totalrup = ir_gabung.assign(TOTAL_RUP = lambda x: x.RUP_PENYEDIA + x.RUP_SWAKELOLA)
    ir_gabung_selisih = ir_gabung_totalrup.assign(SELISIH = lambda x: x.STRUKTUR_ANGGARAN - x.RUP_PENYEDIA - x.RUP_SWAKELOLA) 
    ir_gabung_final = ir_gabung_selisih.assign(PERSEN = lambda x: round(((x.RUP_PENYEDIA + x.RUP_SWAKELOLA) / x.STRUKTUR_ANGGARAN * 100), 2))

    ### Download data % INPUT RUP
    unduh_perseninputrup = unduh_data(ir_gabung_final)
    st.download_button(
        label = "📥 Download Data % Input RUP",
        data = unduh_perseninputrup,
        file_name = f"TabelPersenInputRUP-{pilih}.csv",
        mime = "text/csv"
    )

    gd = GridOptionsBuilder.from_dataframe(ir_gabung_final)
    gd.configure_pagination()
    gd.configure_side_bar()
    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    gd.configure_column("STRUKTUR_ANGGARAN", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.STRUKTUR_ANGGARAN.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("RUP_PENYEDIA", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.RUP_PENYEDIA.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("RUP_SWAKELOLA", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.RUP_SWAKELOLA.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("TOTAL_RUP", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.TOTAL_RUP.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("SELISIH", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.SELISIH.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

    gridOptions = gd.build()

    AgGrid(ir_gabung_final, gridOptions=gridOptions, enable_enterprise_modules=True)

## Tab menu Tabel RUP Perangkat Daerah Paket Penyedia
with menu_rup_5:

    st.header(f"TABEL RUP PERANGKAT DAERAH PAKET PENYEDIA TAHUN {tahun}")

    ### Tampilan pilihan menu nama OPD
    opd_tbl_pp = st.selectbox("Pilih Perangkat Daerah :", namaopd, key='menu_rup_5')

    df_RUPPP_PD_tbl = con.execute(f"SELECT * FROM df_RUPPP_umumkan WHERE nama_satker = '{opd_tbl_pp}'").df()

    st.subheader(f"{opd_tbl_pp}")
    
    st.divider()

    sql_query_pp_tbl = """
        SELECT kd_rup AS ID_RUP, nama_satker AS SATUAN_KERJA, nama_paket AS NAMA_PAKET, metode_pengadaan AS METODE_PEMILIHAN, 
        tgl_pengumuman_paket AS TANGGAL_PENGUMUMAN, tgl_awal_pemilihan AS TANGGAL_RENCANA_PEMILIHAN, pagu AS PAGU  
        FROM df_RUPPP_PD_tbl
    """
    df_RUPPP_PD_tbl_tampil = con.execute(sql_query_pp_tbl).df()

    ### Tampilkan data menggunakan AgGrid
    gd = GridOptionsBuilder.from_dataframe(df_RUPPP_PD_tbl_tampil)
    gd.configure_pagination()
    gd.configure_side_bar()
    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    gd.configure_column("PAGU", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.PAGU.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

    gridOptions = gd.build()
    AgGrid(df_RUPPP_PD_tbl_tampil, gridOptions=gridOptions, enable_enterprise_modules=True) 

## Tab menu Tabel RUP Perangkat Daerah Paket Swakelola
with menu_rup_6:
    
    st.header(f"TABEL RUP PERANGKAT DAERAH PAKET SWAKELOLA TAHUN {tahun}")

    ### Tampilan pilihan menu nama OPD
    opd_tbl_ps = st.selectbox("Pilih Perangkat Daerah :", namaopd, key='menu_rup_6')

    df_RUPPS_PD_tbl = con.execute(f"SELECT * FROM df_RUPPS_umumkan WHERE nama_satker = '{opd_tbl_ps}'").df()

    st.subheader(f"{opd_tbl_ps}")

    st.divider()

    sql_query_ps_tbl = """
        SELECT kd_rup AS ID_RUP, nama_satker AS SATUAN_KERJA, nama_paket AS NAMA_PAKET, 
        tgl_pengumuman_paket AS TANGGAL_PENGUMUMAN, tgl_awal_pelaksanaan_kontrak AS TANGGAL_PELAKSANAAN,
        pagu AS PAGU FROM df_RUPPS_PD_tbl
    """
    df_RUPPS_PD_tbl_tampil = con.execute(sql_query_ps_tbl).df()

    ### Tampilkan data menggunakan AgGrid
    gd = GridOptionsBuilder.from_dataframe(df_RUPPS_PD_tbl_tampil)
    gd.configure_pagination()
    gd.configure_side_bar()
    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    gd.configure_column("PAGU", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.PAGU.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

    gridOptions = gd.build()
    AgGrid(df_RUPPS_PD_tbl_tampil, gridOptions=gridOptions, enable_enterprise_modules=True) 