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
DatasetPURCHASINGECAT = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/ECATPaketEpurchasingDetail{tahun}.parquet" 
DatasetPURCHASINGBELA = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/purchasing/BELATokoDaringRealisasi{tahun}.parquet"

## Buat dataframe PURCHASING
try:
    ### Baca file parquet dataset PURCHASING
    df_ECAT = pd.read_parquet(DatasetPURCHASINGECAT)
    df_BELA = pd.read_parquet(DatasetPURCHASINGBELA)

except Exception:
    st.error("Gagal baca dataset PURCHASING")

#####
# Mulai membuat presentasi data Purchasing
#####

# Buat menu yang mau disajikan
menu_purchasing_1, menu_purchasing_2 = st.tabs(["| TRANSAKSI KATALOG |", "| TRANSAKSI TOKO DARING |"])

## Tab menu Transaksi Katalog
with menu_purchasing_1:

    ### Buat tombol unduh dataset
    unduh_ECAT = unduh_data(df_ECAT)

    ecat1, ecat2 = st.columns((8,2))
    with ecat1:
        st.header(f"Transaksi e-Katalog {pilih} Tahun {tahun}")
    with ecat2:
        st.download_button(
            label = "📥 Download Data Transaksi Katalog",
            data = unduh_ECAT,
            file_name = f"ECATPaketEpurchasingDetail-{kodeFolder}-{tahun}.csv",
            mime = "text/csv"
        )

    st.divider()

    status_paket = st.radio("**Status Paket**", ["Paket Selesai", "Paket Proses"], index=None)
    st.write(f"Anda memilih : **{status_paket}**")

    ### Hitung-hitung dataset
    df_ECAT_lokal = con.execute(f"SELECT * FROM df_ECAT WHERE jenis_katalog = 'Lokal' AND nama_sumber_dana = 'APBD' AND paket_status_str = '{status_paket}'").df()
    df_ECAT_sektoral = con.execute(f"SELECT * FROM df_ECAT WHERE jenis_katalog = 'Sektoral' AND paket_status_str = '{status_paket}'").df()
    df_ECAT_nasional = con.execute(f"SELECT * FROM df_ECAT WHERE jenis_katalog = 'Nasional' AND paket_status_str = '{status_paket}'").df()

    jumlah_produk_lokal = df_ECAT_lokal['kd_produk'].unique().shape[0]
    jumlah_penyedia_lokal = df_ECAT_lokal['kd_penyedia'].unique().shape[0]
    jumlah_trx_lokal = df_ECAT_lokal['no_paket'].unique().shape[0]
    nilai_trx_lokal = df_ECAT_lokal['total_harga'].sum()

    colokal1, colokal2, colokal3, colokal4 = st.columns(4)
    colokal1.metric(label="Jumlah Produk Katalog Lokal", value="{:,}".format(jumlah_produk_lokal))
    colokal2.metric(label="Jumlah Penyedia Katalog Lokal", value="{:,}".format(jumlah_penyedia_lokal))
    colokal3.metric(label="Jumlah Transaksi Katalog Lokal", value="{:,}".format(jumlah_trx_lokal))
    colokal4.metric(label="Nilai Transaksi Katalog Lokal", value="{:,.2f}".format(nilai_trx_lokal))
    style_metric_cards()

    jumlah_produk_sektoral = df_ECAT_sektoral['kd_produk'].unique().shape[0]
    jumlah_penyedia_sektoral = df_ECAT_sektoral['kd_penyedia'].unique().shape[0]
    jumlah_trx_sektoral = df_ECAT_sektoral['no_paket'].unique().shape[0]
    nilai_trx_sektoral = df_ECAT_sektoral['total_harga'].sum()

    cosektoral1, cosektoral2, cosektoral3, cosektoral4 = st.columns(4)
    cosektoral1.metric(label="Jumlah Produk Katalog Sektoral", value="{:,}".format(jumlah_produk_sektoral))
    cosektoral2.metric(label="Jumlah Penyedia Katalog Sektoral", value="{:,}".format(jumlah_penyedia_sektoral))
    cosektoral3.metric(label="Jumlah Transaksi Katalog Sektoral", value="{:,}".format(jumlah_trx_sektoral))
    cosektoral4.metric(label="Nilai Transaksi Katalog Sektoral", value="{:,.2f}".format(nilai_trx_sektoral))
    style_metric_cards()

    jumlah_produk_nasional = df_ECAT_nasional['kd_produk'].unique().shape[0]
    jumlah_penyedia_nasional = df_ECAT_nasional['kd_penyedia'].unique().shape[0]
    jumlah_trx_nasional = df_ECAT_nasional['no_paket'].unique().shape[0]
    nilai_trx_nasional = df_ECAT_nasional['total_harga'].sum()

    conasional1, conasional2, conasional3, conasional4 = st.columns(4)
    conasional1.metric(label="Jumlah Produk Katalog Nasional", value="{:,}".format(jumlah_produk_nasional))
    conasional2.metric(label="Jumlah Penyedia Katalog Nasional", value="{:,}".format(jumlah_penyedia_nasional))
    conasional3.metric(label="Jumlah Transaksi Katalog Nasional", value="{:,}".format(jumlah_trx_nasional))
    conasional4.metric(label="Nilai Transaksi Katalog Nasional", value="{:,.2f}".format(nilai_trx_nasional))
    style_metric_cards()

    st.divider()

    ### Buat grafik e-Purchasing 
    grafik_ecat_11, grafik_ecat_12, grafik_ecat_13, grafik_ecat_14 = st.tabs(["| Grafik Jumlah Produk Katalog |", "| Grafik Jumlah Penyedia Katalog |", "| Grafik Jumlah Transaksi Katalog |", "| Grafik Nilai Transaksi Katalog |"])
    
    with grafik_ecat_11:

        st.subheader("Grafik Jumlah Produk Katalog")

        #### Query data grafik Jumlah Produk Katalog

        sql_jumlah_produk = f"""
            SELECT jenis_katalog AS JENIS_KATALOG,  COUNT(DISTINCT(kd_produk)) AS JUMLAH_PRODUK
            FROM df_ECAT WHERE nama_sumber_dana = 'APBD' AND paket_status_str = '{status_paket}'
            GROUP BY JENIS_KATALOG
        """

        tabel_jumlah_produk = con.execute(sql_jumlah_produk).df()

        st.bar_chart(tabel_jumlah_produk, x="JENIS_KATALOG", y="JUMLAH_PRODUK", color="JENIS_KATALOG")

    with grafik_ecat_12:

        st.subheader("Grafik Jumlah Penyedia Katalog")

        #### Query data grafik Jumlah Penyedia Katalog

        sql_jumlah_penyedia = f"""
            SELECT jenis_katalog AS JENIS_KATALOG,  COUNT(DISTINCT(kd_penyedia)) AS JUMLAH_PENYEDIA
            FROM df_ECAT WHERE nama_sumber_dana = 'APBD' AND paket_status_str = '{status_paket}'
            GROUP BY JENIS_KATALOG
        """

        tabel_jumlah_penyedia = con.execute(sql_jumlah_penyedia).df()

        st.bar_chart(tabel_jumlah_penyedia, x="JENIS_KATALOG", y="JUMLAH_PENYEDIA", color="JENIS_KATALOG")

    with grafik_ecat_13:

        st.subheader("Grafik Jumlah Transaksi Katalog")

        #### Query data grafik Jumlah Transaksi Katalog

        sql_jumlah_transaksi = f"""
            SELECT jenis_katalog AS JENIS_KATALOG,  COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
            FROM df_ECAT WHERE nama_sumber_dana = 'APBD' AND paket_status_str = '{status_paket}'
            GROUP BY JENIS_KATALOG
        """

        tabel_jumlah_transaksi = con.execute(sql_jumlah_transaksi).df()

        st.bar_chart(tabel_jumlah_transaksi, x="JENIS_KATALOG", y="JUMLAH_TRANSAKSI", color="JENIS_KATALOG")

    with grafik_ecat_14:

        st.subheader("Grafik Nilai Transaksi Katalog")

        #### Query data grafik Nilai Transaksi Katalog

        sql_nilai_transaksi = f"""
            SELECT jenis_katalog AS JENIS_KATALOG,  SUM(total_harga) AS NILAI_TRANSAKSI
            FROM df_ECAT WHERE nama_sumber_dana = 'APBD' AND paket_status_str = '{status_paket}'
            GROUP BY JENIS_KATALOG
        """

        tabel_nilai_transaksi = con.execute(sql_nilai_transaksi).df()

        st.bar_chart(tabel_nilai_transaksi, x="JENIS_KATALOG", y="NILAI_TRANSAKSI", color="JENIS_KATALOG")

    st.divider()

    grafik_ecat_21, grafik_ecat_22 = st.tabs(["| Grafik Jumlah Transaksi e-Katalog Lokal Perangkat Daerah |", "| Grafik Nilai Transaksi e-Katalog Lokal Perangkat Daerah |"])

    with grafik_ecat_21:

        #### Query data grafik jumlah Transaksi Katalog Lokal Perangkat Daerah

        sql_jumlah_transaksi_lokal_pd = """
            SELECT nama_satker AS NAMA_SATKER, COUNT(DISTINCT(no_paket)) AS JUMLAH_TRANSAKSI
            FROM df_ECAT_lokal WHERE NAMA_SATKER IS NOT NULL 
            GROUP BY NAMA_SATKER ORDER BY JUMLAH_TRANSAKSI DESC
        """

        tabel_jumlah_transaksi_lokal_pd = con.execute(sql_jumlah_transaksi_lokal_pd).df()

        grafik_ecat_21_1, grafik_ecat_21_2 = st.columns((4,6))

        with grafik_ecat_21_1:
            
            AgGrid(tabel_jumlah_transaksi_lokal_pd, key="trxlokal")
            
        with grafik_ecat_21_2:

            grafik_jumlah_transaksi_katalog_lokal = px.bar(tabel_jumlah_transaksi_lokal_pd, x='NAMA_SATKER', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi e-Katalog Lokal Perangkat Daerah')
            grafik_jumlah_transaksi_katalog_lokal.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            st.plotly_chart(grafik_jumlah_transaksi_katalog_lokal, theme="streamlit", use_container_width=True)

    with grafik_ecat_22:

        #### Query data grafik nilai Transaksi Katalog Lokal Perangkat Daerah

        sql_nilai_transaksi_lokal_pd = """
            SELECT nama_satker AS NAMA_SATKER, SUM(total_harga) AS NILAI_TRANSAKSI
            FROM df_ECAT_lokal WHERE NAMA_SATKER IS NOT NULL
            GROUP BY NAMA_SATKER ORDER BY NILAI_TRANSAKSI DESC
        """

        tabel_nilai_transaksi_lokal_pd = con.execute(sql_nilai_transaksi_lokal_pd).df()

        grafik_ecat_22_1, grafik_ecat_22_2 = st.columns((4,6))

        with grafik_ecat_22_1:

            gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_lokal_pd)
            gd.configure_pagination()
            gd.configure_side_bar()
            gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

            gridOptions = gd.build()
            AgGrid(tabel_nilai_transaksi_lokal_pd, key="nilailokal", gridOptions=gridOptions, enable_enterprise_modules=True)

        with grafik_ecat_22_2:
            
            grafik_nilai_transaksi_katalog_lokal = px.bar(tabel_nilai_transaksi_lokal_pd, x='NAMA_SATKER', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi e-Katalog Lokal Perangkat Daerah')
            grafik_nilai_transaksi_katalog_lokal.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            st.plotly_chart(grafik_nilai_transaksi_katalog_lokal, theme="streamlit", use_container_width=True)

## Tab menu Transaksi Toko Daring
with menu_purchasing_2:

    ### Buat tombol unduh dataset
    unduh_BELA = unduh_data(df_BELA)

    bela1, bela2 = st.columns((8,2))
    with bela1:
        st.header(f"Transaksi Toko Daring {pilih} Tahun {tahun}")
    with bela2:
        st.download_button(
            label = "📥 Download Data Transaksi Katalog",
            data = unduh_BELA,
            file_name = f"BELATokoDaringRealisasi-{kodeFolder}-{tahun}.csv",
            mime = "text/csv"
        )

    st.divider()

    status_verifikasi = st.radio("**Status Verifikasi Transaksi**", ["verified", "unverified"], index=None)
    st.write(f"Anda memilih : **{status_verifikasi}**")

    ### Hitung-hitungan dataset
    df_BELA_filter = con.execute(f"SELECT * FROM df_BELA WHERE nama_satker IS NOT NULL AND status_verif = '{status_verifikasi}'").df()

    jumlah_trx_daring = df_BELA_filter['order_id'].unique().shape[0]
    nilai_trx_daring = df_BELA_filter['valuasi'].sum()

    cobela1, cobela2, cobela3, cobela4 = st.columns(4)
    cobela1.subheader("")
    cobela2.metric(label="Jumlah Transaksi Toko Daring", value="{:,}".format(jumlah_trx_daring))
    cobela3.metric(label="Nilai Transaksi Toko Daring", value="{:,.2f}".format(nilai_trx_daring))
    cobela4.subheader("")
    style_metric_cards()

    st.divider()

    grafik_bela_11, grafik_bela_12 = st.tabs(["| Grafik Jumlah Transaksi Toko Daring Perangkat Daerah |", "| Grafik Nilai Transaksi Toko Daring Perangkat Daerah |"])

    with grafik_bela_11:

        st.subheader("Grafik Jumlah Transaksi Toko Daring Perangkat Daerah")

        #### Query data grafik jumlah Transaksi Toko Daring Perangkat Daerah

        sql_jumlah_transaksi_bela_pd = """
            SELECT nama_satker AS NAMA_SATKER, COUNT(DISTINCT(order_id)) AS JUMLAH_TRANSAKSI
            FROM df_BELA_filter WHERE NAMA_SATKER IS NOT NULL
            GROUP BY NAMA_SATKER ORDER BY JUMLAH_TRANSAKSI DESC
        """

        tabel_jumlah_transaksi_bela_pd = con.execute(sql_jumlah_transaksi_bela_pd).df()

        grafik_bela_11_1, grafik_bela_11_2 = st.columns((4,6))

        with grafik_bela_11_1:

            AgGrid(tabel_jumlah_transaksi_bela_pd, key="trxbela")

        with grafik_bela_11_2:

            grafik_jumlah_transaksi_bela = px.bar(tabel_jumlah_transaksi_bela_pd, x='NAMA_SATKER', y='JUMLAH_TRANSAKSI', text_auto='.2s', title='Grafik Jumlah Transaksi Toko Daring Perangkat Daerah')
            grafik_jumlah_transaksi_bela.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            st.plotly_chart(grafik_jumlah_transaksi_bela, theme="streamlit", use_container_width=True)

    with grafik_bela_12:

        st.subheader("Grafik Nilai Transaksi Toko Daring Perangkat Daerah")

        #### Query data grafik nilai Transaksi Toko Daring Perangkat Daerah

        sql_nilai_transaksi_bela_pd = """
            SELECT nama_satker AS NAMA_SATKER, SUM(valuasi) AS NILAI_TRANSAKSI
            FROM df_BELA_filter WHERE NAMA_SATKER IS NOT NULL
            GROUP BY NAMA_SATKER ORDER BY NILAI_TRANSAKSI DESC
        """

        tabel_nilai_transaksi_bela_pd = con.execute(sql_nilai_transaksi_bela_pd).df()

        grafik_bela_12_1, grafik_bela_12_2 = st.columns((4,6))

        with grafik_bela_12_1:

            gd = GridOptionsBuilder.from_dataframe(tabel_nilai_transaksi_bela_pd)
            gd.configure_pagination()
            gd.configure_side_bar()
            gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            gd.configure_column("NILAI_TRANSAKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TRANSAKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

            gridOptions = gd.build()
            AgGrid(tabel_nilai_transaksi_bela_pd, key="nilaibela", gridOptions=gridOptions, enable_enterprise_modules=True)

        with grafik_bela_12_2:

            grafik_nilai_transaksi_bela = px.bar(tabel_nilai_transaksi_bela_pd, x='NAMA_SATKER', y='NILAI_TRANSAKSI', text_auto='.2s', title='Grafik Nilai Transaksi Toko Daring Perangkat Daerah')
            grafik_nilai_transaksi_bela.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            st.plotly_chart(grafik_nilai_transaksi_bela, theme="streamlit", use_container_width=True)