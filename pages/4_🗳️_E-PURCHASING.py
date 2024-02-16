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
import io
import xlsxwriter
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
# Import fungsi pribadi
from fungsi import *

# App Logo
logo()

# Konfigurasi variabel lokasi UKPBJ
daerah =    ["PROV. KALBAR", "KAB. BENGKAYANG", "KAB. MELAWI", "KOTA PONTIANAK", "KAB. SANGGAU", "KAB. SEKADAU", "KAB. KAPUAS HULU", "KAB. KUBU RAYA", "KAB. LANDAK", "KOTA SINGKAWANG", 
             "KAB. SINTANG", "KAB. MEMPAWAH", "KAB. KETAPANG", "KAB. KATINGAN", "KAB. SUMEDANG", "KAB. PARIGI MOUTONG"]

tahuns = ["2024", "2023", "2022"]

pilih = st.sidebar.selectbox("Pilih UKPBJ yang diinginkan :", daerah)
tahun = st.sidebar.selectbox("Pilih Tahun :", tahuns)

if pilih == "PROV. KALBAR":
    kodeFolder = "prov"
    kodeRUP = "D197"
    kodeLPSE = "97"
if pilih == "KAB. BENGKAYANG":
    kodeFolder = "bky"
    kodeRUP = "D206"
    kodeLPSE = "444"
if pilih == "KAB. MELAWI":
    kodeFolder = "mlw"
    kodeRUP = "D210"
    kodeLPSE = "540"
if pilih == "KOTA PONTIANAK":
    kodeFolder = "ptk"
    kodeRUP = "D199"
    kodeLPSE = "62"
if pilih == "KAB. SANGGAU":
    kodeFolder = "sgu"
    kodeRUP = "D204"
    kodeLPSE = "298"
if pilih == "KAB. SEKADAU":
    kodeFolder = "skd"
    kodeRUP = "D198"
    kodeLPSE = "175"
if pilih == "KAB. KAPUAS HULU":
    kodeFolder = "kph"
    kodeRUP = "D209"
    kodeLPSE = "488"
if pilih == "KAB. KUBU RAYA":
    kodeFolder = "kkr"
    kodeRUP = "D202"
    kodeLPSE = "188"
if pilih == "KAB. LANDAK":
    kodeFolder = "ldk"
    kodeRUP = "D205"
    kodeLPSE = "496"
if pilih == "KOTA SINGKAWANG":
    kodeFolder = "skw"
    kodeRUP = "D200"
    kodeLPSE = "132"
if pilih == "KAB. SINTANG":
    kodeFolder = "stg"
    kodeRUP = "D211"
    kodeLPSE = "345"
if pilih == "KAB. MEMPAWAH":
    kodeFolder = "mpw"
    kodeRUP = "D552"
    kodeLPSE = "118"
if pilih == "KAB. KETAPANG":
    kodeFolder = "ktp"
    kodeRUP = "D201"
    kodeLPSE = "110"
if pilih == "KAB. KATINGAN":
    kodeFolder = "ktn"
    kodeRUP = "D236"
    kodeLPSE = "438"
if pilih == "KAB. SUMEDANG":
    kodeFolder = "smd"
    kodeRUP = "D118"
    kodeLPSE = "432"
if pilih == "KAB. PARIGI MOUTONG":
    kodeFolder = "prg"
    kodeRUP = "D423"
    kodeLPSE = "149"

# Persiapan Dataset
# con = duckdb.connect(database=':memory:')
duckdb.sql("INSTALL httpfs")
duckdb.sql("LOAD httpfs")

## Akses file dataset format parquet dari Google Cloud Storage via URL public
#DatasetPURCHASINGECAT = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/ECATPaketEpurchasing{tahun}.parquet" 
#DatasetPURCHASINGBELA = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/BELATokoDaringRealisasi{tahun}.parquet"
#DatasetPURCHASINGECATKD = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/ECATKomoditasDetail{tahun}.parquet"
#DatasetPURCHASINGECATIS = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/ECATInstansiSatker{tahun}.parquet"
#DatasetPURCHASINGECATPD = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/ECATPenyediaDetail{tahun}.xlsx"

## Akses file dataset https://data.pbj.my.id
DatasetPURCHASINGECAT = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Ecat-PaketEPurchasing{tahun}.parquet"
DatasetPURCHASINGBELA = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Bela-TokoDaringRealisasi{tahun}.parquet"
DatasetPURCHASINGECATKD = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/ECATKomoditasDetail{tahun}.parquet"
DatasetPURCHASINGECATIS = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Ecat-InstansiSatker.parquet"
DatasetPURCHASINGECATPD = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/ECATPenyediaDetail{tahun}.parquet"

## Akses file Excel dataset https://data.pbj.my.id
#DatasetPURCHASINGECAT = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Ecat-PaketEPurchasing{tahun}.xlsx"
#DatasetPURCHASINGBELA = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Bela-TokoDaringRealisasi{tahun}.xlsx"
#DatasetPURCHASINGECATKD = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/ECATKomoditasDetail{tahun}.xlsx"
#DatasetPURCHASINGECATIS = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Ecat-InstansiSatker.xlsx"
#DatasetPURCHASINGECATPD = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/ECATPenyediaDetail{tahun}.xlsx"

#####
# Mulai membuat presentasi data Purchasing
#####

# Buat menu yang mau disajikan
menu_purchasing_1, menu_purchasing_2 = st.tabs(["| KATALOG |", "| TOKO DARING |"])

## Tab menu Transaksi Katalog
with menu_purchasing_1:

    menu_purchasing_1_1, menu_purchasing_1_2, menu_purchasing_1_3 = st.tabs(["| TRANSAKSI KATALOG |", "| TRANSAKSI KATALOG (ETALASE) |", "| TABEL NILAI ETALASE |"])

    try:

        ### Tarik dataset df_ECAT, df_ECAT_KD, df_ECAT_IS dan df_ECATPD
        df_ECAT = tarik_data(DatasetPURCHASINGECAT)
        df_ECAT_KD = tarik_data(DatasetPURCHASINGECATKD)
        df_ECAT_IS = tarik_data(DatasetPURCHASINGECATIS)
        df_ECAT_PD = tarik_data(DatasetPURCHASINGECATPD)

        ## Gabung dataframe Katalog + Katalog Komoditas Detail + Katalog Instansi Satker + Katalog Penyedia Detail
        df_ECAT_0 = df_ECAT.merge(df_ECAT_KD, how='left', on='kd_komoditas').drop('nama_satker', axis=1)
        df_ECAT_1 = pd.merge(df_ECAT_0, df_ECAT_IS, left_on='satker_id', right_on='kd_satker', how='left')
        df_ECAT_OK = df_ECAT_1.merge(df_ECAT_PD, how='left', on='kd_penyedia')

        ### Buat tombol unduh dataset
        unduh_ECAT_excel = download_excel(df_ECAT_OK)

        with menu_purchasing_1_1:

            ecat1, ecat2 = st.columns((8,2))
            with ecat1:
                st.header(f"TRANSAKSI E-KATALOG - {pilih} - TAHUN {tahun}")
            with ecat2:
                st.download_button(
                    label = "📥 Data Tramsaksi E-Katalog",
                    data = unduh_ECAT_excel,
                    file_name = f"TransaksiEKATALOG-{kodeFolder}-{tahun}.xlsx",
                    mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )

            st.divider()

            KATALOG_radio_1, KATALOG_radio_2, KATALOG_radio_3, KATALOG_radio_4 = st.columns((1,1,2,6))
            with KATALOG_radio_1:
                jenis_katalog = st.radio("**Jenis Katalog**", ["Lokal", "Nasional", "Sektoral", "Gabungan"])
            with KATALOG_radio_2:
                #nama_sumber_dana = st.radio("**Sumber Dana**", df_ECAT_OK['nama_sumber_dana'].unique())    
                nama_sumber_dana = st.radio("**Sumber Dana**", ["APBD", "APBDP", "APBN", "APBNP", "BLUD", "BLU", "BUMN", "BUMD"])
            with KATALOG_radio_3:
                status_paket = st.radio("**Status Paket**", ["Paket Selesai", "Paket Proses", "Gabungan"])
            st.write(f"Anda memilih : **{status_paket}** dan **{jenis_katalog}** dan **{nama_sumber_dana}**")

            ### Hitung-hitung dataset Katalog
            if (jenis_katalog == "Gabungan" and status_paket == "Gabungan"):
                df_ECAT_filter = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana}'").df()
            elif jenis_katalog == "Gabungan":
                df_ECAT_filter = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana}' AND paket_status_str = '{status_paket}'").df()
            elif status_paket == "Gabungan":
                df_ECAT_filter = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana}' AND jenis_katalog = '{jenis_katalog}'").df()
            else:    
                df_ECAT_filter = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana}' AND jenis_katalog = '{jenis_katalog}' AND paket_status_str = '{status_paket}'").df()

            jumlah_produk = df_ECAT_filter['kd_produk'].unique().shape[0]
            jumlah_penyedia = df_ECAT_filter['kd_penyedia'].unique().shape[0]
            jumlah_trx = df_ECAT_filter['no_paket'].unique().shape[0]
            nilai_trx = df_ECAT_filter['total_harga'].sum()

            colokal1, colokal2, colokal3, colokal4 = st.columns(4)
            colokal1.metric(label="Jumlah Produk Katalog", value="{:,}".format(jumlah_produk))
            colokal2.metric(label="Jumlah Penyedia Katalog", value="{:,}".format(jumlah_penyedia))
            colokal3.metric(label="Jumlah Transaksi Katalog", value="{:,}".format(jumlah_trx))
            colokal4.metric(label="Nilai Transaksi Katalog", value="{:,.2f}".format(nilai_trx))
            style_metric_cards()

            st.divider()

            st.subheader("Berdasarkan Kualifikasi Usaha")

            ### Buat grafik Katalog Penyedia UKM
            grafik_ukm_tab_1, grafik_ukm_tab_2 = st.tabs(["| Jumlah Transaksi Penyedia |", "| Nilai Transaksi Penyedia |"])

            with grafik_ukm_tab_1:

                #### Query data grafik jumlah transaksi penyedia ukm
                sql_jumlah_ukm = f"""
                    SELECT penyedia_ukm AS PENYEDIA_UKM, COUNT(DISTINCT(kd_penyedia)) AS JUMLAH_UKM
                    FROM df_ECAT_filter GROUP BY PENYEDIA_UKM
                """ 

                tabel_jumlah_ukm = duckdb.sql(sql_jumlah_ukm).df()
                
                grafik_ukm_tab_1_1, grafik_ukm_tab_1_2 = st.columns((3,7))
                
                with grafik_ukm_tab_1_1:

                    AgGrid(tabel_jumlah_ukm)

                with grafik_ukm_tab_1_2:

                    fig_katalog_jumlah_ukm = px.pie(tabel_jumlah_ukm, values='JUMLAH_UKM', names="PENYEDIA_UKM", title='Grafik Jumlah Transaksi Katalog PENYEDIA UKM', hole=.3)
                    st.plotly_chart(fig_katalog_jumlah_ukm, theme='streamlit', use_container_width=True)           

            with grafik_ukm_tab_2:

                #### Query data grafik nilai transaksi penyedia ukm
                sql_nilai_ukm = f"""
                    SELECT penyedia_ukm AS PENYEDIA_UKM, SUM(total_harga) AS NILAI_UKM
                    FROM df_ECAT_filter GROUP BY PENYEDIA_UKM
                """ 

                tabel_nilai_ukm = duckdb.sql(sql_nilai_ukm).df()
                
                grafik_ukm_tab_2_1, grafik_ukm_tab_2_2 = st.columns((3.5,6.5))
                
                with grafik_ukm_tab_2_1:

                    gd = GridOptionsBuilder.from_dataframe(tabel_nilai_ukm)
                    gd.configure_pagination()
                    gd.configure_side_bar()
                    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                    gd.configure_column("NILAI_UKM", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_UKM.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                    gridOptions = gd.build()
                    AgGrid(tabel_nilai_ukm, gridOptions=gridOptions, enable_enterprise_modules=True)

                with grafik_ukm_tab_2_2:

                    fig_katalog_nilai_ukm = px.pie(tabel_nilai_ukm, values='NILAI_UKM', names="PENYEDIA_UKM", title='Grafik Nilai Transaksi Katalog PENYEDIA UKM', hole=.3)
                    st.plotly_chart(fig_katalog_nilai_ukm, theme='streamlit', use_container_width=True)           

            st.divider()

            st.subheader("Berdasarkan Nama Komoditas")

            grafik_ecat_nk_1, grafik_ecat_nk_2 = st.tabs(["| Jumlah Transaksi Tiap Komoditas |", "| Nilai Transaksi Tiap Komoditas |"])

            with grafik_ecat_nk_1:

                #### Query data grafik jumlah Transaksi Katalog Lokal berdasarkan Nama Komoditas
                if jenis_katalog == "Lokal":
                    sql_jumlah_transaksi_lokal_nk = f"""
                        SELECT nama_komoditas AS NAMA_KOMODITAS, COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
                        FROM df_ECAT_filter WHERE NAMA_KOMODITAS IS NOT NULL AND kd_instansi_katalog = '{kodeRUP}'
                        GROUP BY NAMA_KOMODITAS ORDER BY JUMLAH_TRANSAKSI DESC
                    """
                else:
                    sql_jumlah_transaksi_lokal_nk = f"""
                        SELECT nama_komoditas AS NAMA_KOMODITAS, COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
                        FROM df_ECAT_filter WHERE NAMA_KOMODITAS IS NOT NULL 
                        GROUP BY NAMA_KOMODITAS ORDER BY JUMLAH_TRANSAKSI DESC
                    """

                tabel_jumlah_transaksi_lokal_nk = duckdb.sql(sql_jumlah_transaksi_lokal_nk).df()

                grafik_ecat_nk_11, grafik_ecat_nk_12 = st.columns((4,6))

                with grafik_ecat_nk_11:
                    
                    AgGrid(tabel_jumlah_transaksi_lokal_nk)
                    
                with grafik_ecat_nk_12:

                    grafik_jumlah_transaksi_katalog_lokal_nk = px.bar(tabel_jumlah_transaksi_lokal_nk, x='NAMA_KOMODITAS', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi e-Katalog Lokal - Nama Komoditas')
                    grafik_jumlah_transaksi_katalog_lokal_nk.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_jumlah_transaksi_katalog_lokal_nk, theme="streamlit", use_container_width=True)

            with grafik_ecat_nk_2:

                #### Query data grafik nilai Transaksi Katalog Lokal berdasarkan Nama Komoditas
                if jenis_katalog == "Lokal":
                    sql_nilai_transaksi_lokal_nk = f"""
                        SELECT nama_komoditas AS NAMA_KOMODITAS, SUM(total_harga) AS NILAI_TRANSAKSI
                        FROM df_ECAT_filter WHERE NAMA_KOMODITAS IS NOT NULL AND kd_instansi_katalog = '{kodeRUP}'
                        GROUP BY NAMA_KOMODITAS ORDER BY NILAI_TRANSAKSI DESC
                    """
                else:
                    sql_nilai_transaksi_lokal_nk = f"""
                        SELECT nama_komoditas AS NAMA_KOMODITAS, SUM(total_harga) AS NILAI_TRANSAKSI
                        FROM df_ECAT_filter WHERE NAMA_KOMODITAS IS NOT NULL
                        GROUP BY NAMA_KOMODITAS ORDER BY NILAI_TRANSAKSI DESC
                    """

                tabel_nilai_transaksi_lokal_nk = duckdb.sql(sql_nilai_transaksi_lokal_nk).df()

                grafik_ecat_nk_21, grafik_ecat_nk_22 = st.columns((4,6))

                with grafik_ecat_nk_21:

                    gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_lokal_nk)
                    gd.configure_pagination()
                    gd.configure_side_bar()
                    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                    gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                    gridOptions = gd.build()
                    AgGrid(tabel_nilai_transaksi_lokal_nk, gridOptions=gridOptions, enable_enterprise_modules=True)

                with grafik_ecat_nk_22:
                    
                    grafik_nilai_transaksi_katalog_lokal_nk = px.bar(tabel_nilai_transaksi_lokal_nk, x='NAMA_KOMODITAS', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi e-Katalog Lokal - Nama Komoditas')
                    grafik_nilai_transaksi_katalog_lokal_nk.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_nilai_transaksi_katalog_lokal_nk, theme="streamlit", use_container_width=True)

            st.divider()

            st.subheader("Berdasarkan Perangkat Daerah (10 Besar)")

            grafik_ecat_pd_1, grafik_ecat_pd_2 = st.tabs(["| Jumlah Transaksi Perangkat Daerah |", "| Nilai Transaksi Perangkat Daerah |"])

            with grafik_ecat_pd_1:

                #### Query data grafik jumlah Transaksi Katalog Lokal Perangkat Daerah
                sql_jumlah_transaksi_lokal_pd = """
                    SELECT nama_satker AS NAMA_SATKER, COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
                    FROM df_ECAT_filter WHERE NAMA_SATKER IS NOT NULL 
                    GROUP BY NAMA_SATKER ORDER BY JUMLAH_TRANSAKSI DESC LIMIT 10
                """

                tabel_jumlah_transaksi_lokal_pd = duckdb.sql(sql_jumlah_transaksi_lokal_pd).df()

                grafik_ecat_pd_11, grafik_ecat_pd_12 = st.columns((4,6))

                with grafik_ecat_pd_11:
                    
                    AgGrid(tabel_jumlah_transaksi_lokal_pd)
                    
                with grafik_ecat_pd_12:

                    grafik_jumlah_transaksi_katalog_lokal_pd = px.bar(tabel_jumlah_transaksi_lokal_pd, x='NAMA_SATKER', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi e-Katalog Lokal Perangkat Daerah')
                    grafik_jumlah_transaksi_katalog_lokal_pd.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_jumlah_transaksi_katalog_lokal_pd, theme="streamlit", use_container_width=True)

            with grafik_ecat_pd_2:

                #### Query data grafik nilai Transaksi Katalog Lokal Perangkat Daerah
                sql_nilai_transaksi_lokal_pd = """
                    SELECT nama_satker AS NAMA_SATKER, SUM(total_harga) AS NILAI_TRANSAKSI
                    FROM df_ECAT_filter WHERE NAMA_SATKER IS NOT NULL
                    GROUP BY NAMA_SATKER ORDER BY NILAI_TRANSAKSI DESC LIMIT 10
                """

                tabel_nilai_transaksi_lokal_pd = duckdb.sql(sql_nilai_transaksi_lokal_pd).df()

                grafik_ecat_pd_21, grafik_ecat_pd_22 = st.columns((4,6))

                with grafik_ecat_pd_21:

                    gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_lokal_pd)
                    gd.configure_pagination()
                    gd.configure_side_bar()
                    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                    gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                    gridOptions = gd.build()
                    AgGrid(tabel_nilai_transaksi_lokal_pd, gridOptions=gridOptions, enable_enterprise_modules=True)

                with grafik_ecat_pd_22:
                    
                    grafik_nilai_transaksi_katalog_lokal = px.bar(tabel_nilai_transaksi_lokal_pd, x='NAMA_SATKER', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi e-Katalog Lokal Perangkat Daerah')
                    grafik_nilai_transaksi_katalog_lokal.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_nilai_transaksi_katalog_lokal, theme="streamlit", use_container_width=True)

            st.divider()

            st.subheader("Berdasarkan Pelaku Usaha (10 Besar)")

            grafik_ecat_pu_1, grafik_ecat_pu_2 = st.tabs(["| Jumlah Transaksi Pelaku Usaha |", "| Nilai Transaksi Pelaku Usaha |"])

            with grafik_ecat_pu_1:

                #### Query data grafik jumlah Transaksi Katalog Lokal Pelaku Usaha
                sql_jumlah_transaksi_ecat_pu = """
                    SELECT nama_penyedia AS NAMA_PENYEDIA, COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
                    FROM df_ECAT_filter WHERE NAMA_PENYEDIA IS NOT NULL 
                    GROUP BY NAMA_PENYEDIA ORDER BY JUMLAH_TRANSAKSI DESC LIMIT 10
                """

                tabel_jumlah_transaksi_ecat_pu = duckdb.sql(sql_jumlah_transaksi_ecat_pu).df()

                grafik_ecat_pu_1_1, grafik_ecat_pu_1_2 = st.columns((4,6))

                with grafik_ecat_pu_1_1:
                    
                    AgGrid(tabel_jumlah_transaksi_ecat_pu)
                    
                with grafik_ecat_pu_1_2:

                    grafik_jumlah_transaksi_ecat_pu = px.bar(tabel_jumlah_transaksi_ecat_pu, x='NAMA_PENYEDIA', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi Katalog Pelaku Usaha')
                    grafik_jumlah_transaksi_ecat_pu.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_jumlah_transaksi_ecat_pu, theme="streamlit", use_container_width=True)

            with grafik_ecat_pu_2:

                #### Query data grafik nilai Transaksi Katalog Lokal Pelaku Usaha
                sql_nilai_transaksi_ecat_pu = """
                    SELECT nama_penyedia AS NAMA_PENYEDIA, SUM(total_harga) AS NILAI_TRANSAKSI
                    FROM df_ECAT_filter WHERE NAMA_PENYEDIA IS NOT NULL
                    GROUP BY NAMA_PENYEDIA ORDER BY NILAI_TRANSAKSI DESC LIMIT 10
                """

                tabel_nilai_transaksi_ecat_pu = duckdb.sql(sql_nilai_transaksi_ecat_pu).df()

                grafik_ecat_pu_2_1, grafik_ecat_pu_2_2 = st.columns((4,6))

                with grafik_ecat_pu_2_1:

                    gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_ecat_pu)
                    gd.configure_pagination()
                    gd.configure_side_bar()
                    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                    gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                    gridOptions = gd.build()
                    AgGrid(tabel_nilai_transaksi_ecat_pu, gridOptions=gridOptions, enable_enterprise_modules=True)

                with grafik_ecat_pu_2_2:
                    
                    grafik_nilai_transaksi_ecat_pu = px.bar(tabel_nilai_transaksi_ecat_pu, x='NAMA_PENYEDIA', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi Katalog Pelaku Usaha')
                    grafik_nilai_transaksi_ecat_pu.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_nilai_transaksi_ecat_pu, theme="streamlit", use_container_width=True)

        with menu_purchasing_1_2:

            etalase1, etalase2 = st.columns((8,2))
            with etalase1:
                st.header(f"TRANSAKSI E-KATALOG (ETALASE) - {pilih} - TAHUN {tahun}")
            with etalase2:
                st.download_button(
                    label = "📥 Data Tramsaksi E-Katalog",
                    data = unduh_ECAT_excel,
                    file_name = f"TransaksiEKATALOG-{kodeFolder}-{tahun}.xlsx",
                    mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    key = "Download Katalog Etalase"
                )

            st.divider()

            ETALASE_radio_1, ETALASE_radio_2, ETALASE_radio_3, ETALASE_radio_4 = st.columns((1,1,2,6))
            with ETALASE_radio_1:
                jenis_katalog_etalase = st.radio("**Jenis Katalog**", ["Lokal", "Nasional", "Sektoral"], key="Etalase_Jenis_Katalog")
            with ETALASE_radio_2:
                nama_sumber_dana_etalase = st.radio("**Sumber Dana**", ["APBD", "APBDP", "APBN", "APBNP", "BLUD", "BLU", "BUMN", "BUMD"], key="Etalase_Sumber_Dana")
            with ETALASE_radio_3:
                status_paket_etalase = st.radio("**Status Paket**", ["Paket Selesai", "Paket Proses", "Gabungan"], key="Etalase_Status_Paket")            

            ### Hitung-hitung dataset Katalog
            # if (jenis_katalog_etalase == "Gabungan" and status_paket_etalase == "Gabungan"):
            #     df_ECAT_ETALASE = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana_etalase}").df()
            # elif jenis_katalog_etalase == "Gabungan":
            #     df_ECAT_ETALASE = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana_etalase}' AND paket_status_str = '{status_paket_etalase}'").df()
            if status_paket_etalase == "Gabungan":
                df_ECAT_ETALASE = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana_etalase}' AND jenis_katalog = '{jenis_katalog_etalase}'").df()
            else:    
                df_ECAT_ETALASE = duckdb.sql(f"SELECT * FROM df_ECAT_OK WHERE nama_sumber_dana = '{nama_sumber_dana_etalase}' AND jenis_katalog = '{jenis_katalog_etalase}' AND paket_status_str = '{status_paket_etalase}'").df()
            ###

            with ETALASE_radio_4:
                nama_komoditas = st.selectbox("Pilih Etalase Belanja :", df_ECAT_ETALASE['nama_komoditas'].unique(), key="Etalase_Nama_Komoditas")
            st.write(f"Anda memilih : **{jenis_katalog_etalase}** dan **{nama_sumber_dana_etalase}** dan **{status_paket_etalase}**")
            
            df_ECAT_ETALASE_filter = duckdb.sql(f"SELECT * FROM df_ECAT_ETALASE WHERE nama_komoditas = '{nama_komoditas}'").df()

            jumlah_produk_etalase = df_ECAT_ETALASE_filter['kd_produk'].unique().shape[0]
            jumlah_penyedia_etalase = df_ECAT_ETALASE_filter['kd_penyedia'].unique().shape[0]
            jumlah_trx_etalase = df_ECAT_ETALASE_filter['no_paket'].unique().shape[0]
            nilai_trx_etalase = df_ECAT_ETALASE_filter['total_harga'].sum()

            coetalase1, coetalase2, coetalase3, coetalase4 = st.columns(4)
            coetalase1.metric(label="Jumlah Produk Katalog", value="{:,}".format(jumlah_produk_etalase))
            coetalase2.metric(label="Jumlah Penyedia Katalog", value="{:,}".format(jumlah_penyedia_etalase))
            coetalase3.metric(label="Jumlah Transaksi Katalog", value="{:,}".format(jumlah_trx_etalase))
            coetalase4.metric(label="Nilai Transaksi Katalog", value="{:,.2f}".format(nilai_trx_etalase))
            style_metric_cards()

            st.divider()

            st.subheader("Berdasarkan Pelaku Usaha (10 Besar)")

            grafik_etalase_pu_1, grafik_etalase_pu_2 = st.tabs(["| Jumlah Transaksi Pelaku Usaha |", "| Nilai Transaksi Pelaku Usaha |"])

            with grafik_etalase_pu_1:
                
                #### Query data grafik jumlah Transaksi Katalog Lokal Pelaku Usaha tiap Etalase
                sql_jumlah_transaksi_ecat_pu_etalase = """
                    SELECT nama_penyedia AS NAMA_PENYEDIA, COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
                    FROM df_ECAT_ETALASE_filter WHERE NAMA_PENYEDIA IS NOT NULL
                    GROUP BY NAMA_PENYEDIA ORDER BY JUMLAH_TRANSAKSI DESC LIMIT 10
                """

                tabel_jumlah_transaksi_ecat_pu_etalase = duckdb.sql(sql_jumlah_transaksi_ecat_pu_etalase).df()

                grafik_etalase_pu_1_1, grafik_etalase_pu_1_2 = st.columns((4,6))

                with grafik_etalase_pu_1_1:

                    AgGrid(tabel_jumlah_transaksi_ecat_pu_etalase)

                with grafik_etalase_pu_1_2:

                    grafik_jumlah_transaksi_ecat_pu_etalase = px.bar(tabel_jumlah_transaksi_ecat_pu_etalase, x='NAMA_PENYEDIA', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi Katalog Pelaku Usaha')
                    grafik_jumlah_transaksi_ecat_pu_etalase.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_jumlah_transaksi_ecat_pu_etalase, theme="streamlit", use_container_width=True)

            with grafik_etalase_pu_2:

                #### Query data grafik nilai Transaksi Katalog Lokal Pelaku Usaha tiap Etalase
                sql_nilai_transaksi_ecat_pu_etalase = """
                    SELECT nama_penyedia AS NAMA_PENYEDIA, SUM(total_harga) AS NILAI_TRANSAKSI
                    FROM df_ECAT_ETALASE_filter WHERE NAMA_PENYEDIA IS NOT NULL
                    GROUP BY NAMA_PENYEDIA ORDER BY NILAI_TRANSAKSI DESC LIMIT 10
                """

                tabel_nilai_transaksi_ecat_pu_etalase = duckdb.sql(sql_nilai_transaksi_ecat_pu_etalase).df()

                grafik_etalase_pu_2_1, grafik_etalase_pu_2_2 = st.columns((4,6))

                with grafik_etalase_pu_2_1:

                    gd_etalase_pu = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_ecat_pu_etalase)
                    gd_etalase_pu.configure_pagination()
                    gd_etalase_pu.configure_side_bar()
                    gd_etalase_pu.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                    gd_etalase_pu.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                    AgGrid(tabel_nilai_transaksi_ecat_pu_etalase, gridOptions=gd_etalase_pu.build(), enable_enterprise_modules=True)

                with grafik_etalase_pu_2_2:

                    grafik_nilai_transaksi_ecat_pu_etalase = px.bar(tabel_nilai_transaksi_ecat_pu_etalase, x='NAMA_PENYEDIA', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi Katalog Pelaku Usaha')
                    grafik_nilai_transaksi_ecat_pu_etalase.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(grafik_nilai_transaksi_ecat_pu_etalase, theme="streamlit", use_container_width=True)

        with menu_purchasing_1_3:

            df_ECAT_OK_PIVOT = duckdb.sql("SELECT nama_komoditas, jenis_katalog, total_harga FROM df_ECAT_OK").df()
            df_ECAT_PIVOT_TABEL = duckdb.sql("PIVOT df_ECAT_OK_PIVOT ON jenis_katalog USING SUM(total_harga)").df().fillna(0)
            df_ECAT_PIVOT_TABEL_OK = duckdb.sql("SELECT nama_komoditas AS NAMA_KOMODITAS, Lokal AS LOKAL, Nasional AS NASIONAL, Sektoral AS SEKTORAL FROM df_ECAT_PIVOT_TABEL").df()

            ### Buat tombol unduh dataset Tabel Nilai Etalase
            unduh_ETALASE_PIVOT_excel = download_excel(df_ECAT_PIVOT_TABEL_OK)

            etalasepivot1, etalasepivot2 = st.columns((8,2))
            with etalasepivot1:
                st.header(f"TABEL NILAI ETALASE - {pilih} - TAHUN {tahun}")
            with etalasepivot2:
                st.download_button(
                    label = "📥 Tabel Nilai Etalase",
                    data = unduh_ETALASE_PIVOT_excel,
                    file_name = f"TabelNilaiEtalase-{kodeFolder}-{tahun}.xlsx",
                    mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )

            st.divider()

            gd_etalase_pivot = GridOptionsBuilder.from_dataframe(df_ECAT_PIVOT_TABEL_OK)
            gd_etalase_pivot.configure_pagination()
            gd_etalase_pivot.configure_side_bar()
            gd_etalase_pivot.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            gd_etalase_pivot.configure_column("LOKAL", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.LOKAL.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 
            gd_etalase_pivot.configure_column("NASIONAL", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NASIONAL.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 
            gd_etalase_pivot.configure_column("SEKTORAL", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.SEKTORAL.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

            AgGrid(df_ECAT_PIVOT_TABEL_OK, gridOptions=gd_etalase_pivot.build(), enable_enterprise_modules=True)

    except Exception:
    
        st.error("Gagal baca dataset E-Katalog")

## Tab menu Transaksi Toko Daring
with menu_purchasing_2:

    try:
        ### Tarik dataset df_BELA
        df_BELA = tarik_data(DatasetPURCHASINGBELA)

        ### Buat tombol unduh dataset
        unduh_BELA_excel = download_excel(df_BELA)

        bela1, bela2 = st.columns((7,3))
        with bela1:
            st.header(f"TRANSAKSI TOKO DARING - {pilih} - TAHUN {tahun}")
        with bela2:
            st.download_button(
                label = "📥 Data Tramsaksi Toko Daring",
                data = unduh_BELA_excel,
                file_name = f"TransaksiTokoDaring-{kodeFolder}-{tahun}.xlsx",
                mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        st.divider()

        status_verifikasi = st.radio("**Status Verifikasi Transaksi**", ["verified", "unverified", "Gabungan"])
        st.write(f"Anda memilih : **{status_verifikasi}**")

        ### Hitung-hitungan dataset
        if status_verifikasi == "Gabungan":
            df_BELA_filter = duckdb.sql(f"SELECT * FROM df_BELA WHERE nama_satker IS NOT NULL").df()
        else:
            df_BELA_filter = duckdb.sql(f"SELECT * FROM df_BELA WHERE nama_satker IS NOT NULL AND status_verif = '{status_verifikasi}'").df()

        jumlah_trx_daring = df_BELA_filter['order_id'].unique().shape[0]
        nilai_trx_daring = df_BELA_filter['valuasi'].sum()

        cobela1, cobela2, cobela3, cobela4 = st.columns(4)
        cobela1.subheader("")
        cobela2.metric(label="Jumlah Transaksi Toko Daring", value="{:,}".format(jumlah_trx_daring))
        cobela3.metric(label="Nilai Transaksi Toko Daring", value="{:,.2f}".format(nilai_trx_daring))
        cobela4.subheader("")
        style_metric_cards()

        st.divider()

        st.subheader("Berdasarkan Perangkat Daerah (10 Besar)")

        grafik_bela_pd_11, grafik_bela_pd_12 = st.tabs(["| Jumlah Transaksi Perangkat Daerah |", "| Nilai Transaksi Perangkat Daerah |"])

        with grafik_bela_pd_11:

            #### Query data grafik jumlah Transaksi Toko Daring Perangkat Daerah
            sql_jumlah_transaksi_bela_pd = """
                SELECT nama_satker AS NAMA_SATKER, COUNT(DISTINCT(order_id)) AS JUMLAH_TRANSAKSI
                FROM df_BELA_filter WHERE NAMA_SATKER IS NOT NULL
                GROUP BY NAMA_SATKER ORDER BY JUMLAH_TRANSAKSI DESC LIMIT 10
            """

            tabel_jumlah_transaksi_bela_pd = duckdb.sql(sql_jumlah_transaksi_bela_pd).df()

            grafik_bela_pd_11_1, grafik_bela_pd_11_2 = st.columns((4,6))

            with grafik_bela_pd_11_1:

                AgGrid(tabel_jumlah_transaksi_bela_pd)

            with grafik_bela_pd_11_2:

                grafik_jumlah_transaksi_bela_pd = px.bar(tabel_jumlah_transaksi_bela_pd, x='NAMA_SATKER', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi Toko Daring Perangkat Daerah')
                grafik_jumlah_transaksi_bela_pd.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                st.plotly_chart(grafik_jumlah_transaksi_bela_pd, theme="streamlit", use_container_width=True)

        with grafik_bela_pd_12:

            #### Query data grafik nilai Transaksi Toko Daring Perangkat Daerah
            sql_nilai_transaksi_bela_pd = """
                SELECT nama_satker AS NAMA_SATKER, SUM(valuasi) AS NILAI_TRANSAKSI
                FROM df_BELA_filter WHERE NAMA_SATKER IS NOT NULL
                GROUP BY NAMA_SATKER ORDER BY NILAI_TRANSAKSI DESC LIMIT 10
            """

            tabel_nilai_transaksi_bela_pd = duckdb.sql(sql_nilai_transaksi_bela_pd).df()

            grafik_bela_pd_12_1, grafik_bela_pd_12_2 = st.columns((4,6))

            with grafik_bela_pd_12_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_bela_pd)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_nilai_transaksi_bela_pd, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_bela_pd_12_2:

                grafik_nilai_transaksi_bela_pd = px.bar(tabel_nilai_transaksi_bela_pd, x='NAMA_SATKER', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi Toko Daring Perangkat Daerah')
                grafik_nilai_transaksi_bela_pd.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                st.plotly_chart(grafik_nilai_transaksi_bela_pd, theme="streamlit", use_container_width=True)

        st.divider()

        st.subheader("Berdasarkan Pelaku Usaha (10 Besar)")

        grafik_bela_pu_11, grafik_bela_pu_12 = st.tabs(["| Jumlah Transaksi Pelaku Usaha |", "| Nilai Transaksi Pelaku Usaha |"])

        with grafik_bela_pu_11:

            #### Query data grafik jumlah Transaksi Toko Daring Pelaku Usaha
            sql_jumlah_transaksi_bela_pu = """
                SELECT nama_merchant AS NAMA_MERCHANT, COUNT(DISTINCT(order_id)) AS JUMLAH_TRANSAKSI
                FROM df_BELA_filter WHERE NAMA_MERCHANT IS NOT NULL
                GROUP BY NAMA_MERCHANT ORDER BY JUMLAH_TRANSAKSI DESC LIMIT 10
            """

            tabel_jumlah_transaksi_bela_pu = duckdb.sql(sql_jumlah_transaksi_bela_pu).df()

            grafik_bela_pu_11_1, grafik_bela_pu_11_2 = st.columns((4,6))

            with grafik_bela_pu_11_1:

                AgGrid(tabel_jumlah_transaksi_bela_pu)

            with grafik_bela_pu_11_2:

                grafik_jumlah_transaksi_bela_pu = px.bar(tabel_jumlah_transaksi_bela_pu, x='NAMA_MERCHANT', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi Toko Daring Pelaku Usaha')
                grafik_jumlah_transaksi_bela_pu.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                st.plotly_chart(grafik_jumlah_transaksi_bela_pu, theme="streamlit", use_container_width=True)

        with grafik_bela_pu_12:

            #### Query data grafik nilai Transaksi Toko Daring Pelaku Usaha
            sql_nilai_transaksi_bela_pu = """
                SELECT nama_merchant AS NAMA_MERCHANT, SUM(valuasi) AS NILAI_TRANSAKSI
                FROM df_BELA_filter WHERE NAMA_MERCHANT IS NOT NULL
                GROUP BY NAMA_MERCHANT ORDER BY NILAI_TRANSAKSI DESC LIMIT 10
            """

            tabel_nilai_transaksi_bela_pu = duckdb.sql(sql_nilai_transaksi_bela_pu).df()

            grafik_bela_pu_12_1, grafik_bela_pu_12_2 = st.columns((4,6))

            with grafik_bela_pu_12_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_bela_pu)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_nilai_transaksi_bela_pu, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_bela_pu_12_2:

                grafik_nilai_transaksi_bela_pu = px.bar(tabel_nilai_transaksi_bela_pu, x='NAMA_MERCHANT', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi Toko Daring Pelaku Usaha')
                grafik_nilai_transaksi_bela_pu.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                st.plotly_chart(grafik_nilai_transaksi_bela_pu, theme="streamlit", use_container_width=True)

    except Exception:
        st.error("Gagal baca dataset Toko Daring")