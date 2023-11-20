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
daerah = ["PROV. KALBAR", "KAB. BENGKAYANG", "KAB. MELAWI", "KOTA PONTIANAK", "KAB. SANGGAU", "KAB. SEKADAU", "KAB. KAPUAS HULU", "KAB. KUBU RAYA", "KAB. LANDAK", "KOTA SINGKAWANG"]

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
if pilih == "KAB. LANDAK":
    kodeFolder = "ldk"
if pilih == "KOTA SINGKAWANG":
    kodeFolder = "skw"

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

### Dataset RUP Master Satker
DatasetRUPMasterSatker = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPMasterSatker{tahun}.parquet"

### Dataset RUP Paket Penyedia Terumumkan
DatasetRUPPP = f"https://storage.googleapis.com/bukanamel/{kodeFolder}/sirup/RUPPaketPenyediaTerumumkan{tahun}.parquet"

## Buat dataframe SPSE
### Baca file parquet dataset SPSE Tender
try:
    df_SPSETenderPengumuman = tarik_data(DatasetSPSETenderPengumuman)
except Exception:
    st.error("Gagal baca dataset SPSE Tender Pengumuman")
try:
    df_SPSETenderSelesai = tarik_data(DatasetSPSETenderSelesai)
except Exception:
    st.error("Gagal baca dataset SPSE Tender Selesai")
try:
    df_SPSETenderSelesaiNilai = tarik_data(DatasetSPSETenderSelesaiNilai)
except Exception:
    st.error("Gagal baca dataset SPSE Tender Selesai Nilai")
try:
    df_SPSETenderSPPBJ = tarik_data(DatasetSPSETenderSPPBJ)
except Exception:
    st.error("Gagal baca dataset SPSE Tender SPPBJ")    
try:    
    df_SPSETenderKontrak = tarik_data(DatasetSPSETenderKontrak)
except Exception:
    st.error("Gagal baca dataset SPSE Tender Kontrak")
try:
    df_SPSETenderSPMK = tarik_data(DatasetSPSETenderSPMK)
except Exception:
    st.error("Gagal baca dataset SPSE Tender SPMK")
try:
    df_SPSETenderBAST = tarik_data(DatasetSPSETenderBAST)
except Exception:
    st.error("Gagal baca dataset SPSE Tender BAST")

### Baca file parquet dataset SPSE Non Tender
try:
    df_SPSENonTenderPengumuman = tarik_data(DatasetSPSENonTenderPengumuman)
except Exception:
    st.error("Gagal baca dataset SPSE Non Tender Pengumuman")
try:
    df_SPSENonTenderSelesai = tarik_data(DatasetSPSENonTenderSelesai)
except Exception:
    st.error("Gagal baca dataset SPSE Non Tender Selesai")
try:
    df_SPSENonTenderSPPBJ = tarik_data(DatasetSPSENonTenderSPPBJ)
except Exception:
    st.error("Gagal baca dataset SPSE Non Tender SPPBJ")
try:
    df_SPSENonTenderKontrak = tarik_data(DatasetSPSENonTenderKontrak)
except Exception:
    st.error("Gagal baca dataset SPSE Non Tender Kontrak")
try:
    df_SPSENonTenderSPMK = tarik_data(DatasetSPSENonTenderSPMK)
except Exception:
    st.error("Gagal baca dataset SPSE Non Tender SPMK")
try:
    df_SPSENonTenderBAST = tarik_data(DatasetSPSENonTenderBAST)
except Exception:
    st.error("Gagal baca dataset SPSE Non Tender BAST")

### Baca file parquet dataset Pencatatan
try:
    df_CatatNonTender = tarik_data(DatasetCatatNonTender)
except Exception:
    st.error("Gagal baca dataset Catat Non Tender")
try:
    df_CatatNonTenderRealisasi = tarik_data(DatasetCatatNonTenderRealisasi)
except Exception:
    st.error("Gagal baca dataset Catat Non Tender Realisasi")
try:
    df_CatatSwakelola = tarik_data(DatasetCatatSwakelola)
except Exception:
    st.error("Gagal baca dataset Catat Swakelola")
try:
    df_CatatSwakelolaRealisasi = tarik_data(DatasetCatatSwakelolaRealisasi)
except Exception:
    st.error("Gagal baca dataset Catat Swakelola Realisasi")

### Baca file parquet dataset Peserta Tender
try:
    df_PesertaTender = tarik_data(DatasetPesertaTender)
except Exception:
    st.error("Gagal baca dataset Peserta Tender")

### Baca file parquet dataset RUP Master Satker
try:
    df_RUPMasterSatker = tarik_data(DatasetRUPMasterSatker)
except Exception:
    st.error("Gagal baca dataset RUP Master Satker")

### Baca file parquet dataset RUP Paket Penyedia Terumumkan
try:
    df_RUPPP = tarik_data(DatasetRUPPP)
    df_RUPPP_umumkan = con.execute("SELECT * FROM df_RUPPP WHERE status_umumkan_rup = 'Terumumkan' AND status_aktif_rup = 'TRUE'").df()
    df_RUPPP_umumkan_filter = df_RUPPP_umumkan[["kd_rup", "status_pdn", "status_ukm"]]
except Exception:
    st.error("Gagal baca dataset RUP Paket Penyedia Terumumkan")

#####
# Mulai membuat presentasi data SPSE
#####

# Buat menu yang mau disajikan
menu_spse_1, menu_spse_2, menu_spse_3, menu_spse_4 = st.tabs(["| TENDER |", "| NON TENDER |", "| PENCATATAN |", "| PESERTA TENDER |"])

## Tab menu SPSE - Tender
with menu_spse_1:

    st.header(f"SPSE - Tender - {pilih}")

    ### Buat dataset gabung df_SPSETenderPengumuman + df_RUPPP_umumkan_filter
    df_SPSETenderPengumuman_OK = df_SPSETenderPengumuman.merge(df_RUPPP_umumkan_filter, how='left', on='kd_rup')

    ### Buat sub menu SPSE - Tender
    menu_spse_1_1, menu_spse_1_2, menu_spse_1_3, menu_spse_1_4, menu_spse_1_5 = st.tabs(["| PENGUMUMAN |", "| SPPBJ |", "| KONTRAK |", "| SPMK |", "| BAPBAST |"])

    #### Tab menu SPSE - Tender - Pengumuman
    with menu_spse_1_1:

        ##### Buat tombol unduh dataset SPSE-Tender-Pengumuman
        unduh_SPSE_Pengumuman = unduh_data(df_SPSETenderPengumuman_OK)
        
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
            sumber_dana_unik = df_SPSETenderPengumuman_OK['sumber_dana'].unique()
            sumber_dana = st.radio("**Sumber Dana**", sumber_dana_unik)
        with SPSE_radio_2:
            status_tender_unik = df_SPSETenderPengumuman_OK['status_tender'].unique()
            status_tender = st.radio("**Status Tender**", status_tender_unik)
        st.write(f"Anda memilih : **{sumber_dana}** dan **{status_tender}**")

        ##### Hitung-hitungan dataset Tender Pengumuman
        df_SPSETenderPengumuman_filter = con.execute(f"SELECT kd_tender, pagu, hps, kualifikasi_paket, jenis_pengadaan, mtd_pemilihan, mtd_evaluasi, mtd_kualifikasi, kontrak_pembayaran, status_pdn, status_ukm FROM df_SPSETenderPengumuman_OK WHERE sumber_dana = '{sumber_dana}' AND status_tender = '{status_tender}'").df()
        jumlah_trx_spse_pengumuman = df_SPSETenderPengumuman_filter['kd_tender'].unique().shape[0]
        nilai_trx_spse_pengumuman_pagu = df_SPSETenderPengumuman_filter['pagu'].sum()
        nilai_trx_spse_pengumuman_hps = df_SPSETenderPengumuman_filter['hps'].sum()

        data_umum_1, data_umum_2, data_umum_3 = st.columns(3)
        data_umum_1.metric(label="Jumlah Tender Diumumkan", value="{:,}".format(jumlah_trx_spse_pengumuman))
        data_umum_2.metric(label="Nilai Pagu Tender Diumumkan", value="{:,.2f}".format(nilai_trx_spse_pengumuman_pagu))
        data_umum_3.metric(label="Nilai HPS Tender Diumumkan", value="{:,.2f}".format(nilai_trx_spse_pengumuman_hps))
        style_metric_cards()

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan Status PDN
        grafik_pdn_1, grafik_pdn_2 = st.tabs(["| Berdasarkan Jumlah Status PDN |", "| Berdasarkan Nilai Status PDN |"])

        with grafik_pdn_1:

            st.subheader("Berdasarkan Jumlah Status PDN")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Status PDN
            
            sql_pdn_jumlah = """
                SELECT status_pdn AS STATUS_PDN, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY STATUS_PDN ORDER BY JUMLAH_PAKET DESC
            """

            tabel_pdn_jumlah_trx = con.execute(sql_pdn_jumlah).df()

            grafik_pdn_1_1, grafik_pdn_1_2 = st.columns((3,7))

            with grafik_pdn_1_1:

                AgGrid(tabel_pdn_jumlah_trx)

            with grafik_pdn_1_2:

                figpdnh = px.pie(tabel_pdn_jumlah_trx, values="JUMLAH_PAKET", names="STATUS_PDN", title='Grafik Status PDN - Jumlah Paket', hole=.3)
                st.plotly_chart(figpdnh, theme="streamlit", use_container_width=True)

        with grafik_pdn_2:

            st.subheader("Berdasarkan Nilai Status PDN")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Status PDN

            sql_pdn_nilai = """
                SELECT status_pdn AS STATUS_PDN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY STATUS_PDN ORDER BY NILAI_PAKET DESC
            """

            tabel_pdn_nilai_trx = con.execute(sql_pdn_nilai).df()

            grafik_pdn_2_1, grafik_pdn_2_2 = st.columns((3,7))

            with grafik_pdn_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_pdn_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_pdn_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)    
                
            with grafik_pdn_2_2:

                figpdnn = px.pie(tabel_pdn_nilai_trx, values="NILAI_PAKET", names="STATUS_PDN", title='Grafik Status PDN - Nilai Paket', hole=.3)
                st.plotly_chart(figpdnn, theme="streamlit", use_container_width=True)

        ####### Grafik jumlah dan nilai transaksi berdasarkan Status UKM
        grafik_ukm_1, grafik_ukm_2 = st.tabs(["| Berdasarkan Jumlah Status UKM |", "| Berdasarkan Nilai Status UKM |"])

        with grafik_ukm_1:

            st.subheader("Berdasarkan Jumlah Status UKM")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Status UKM
            
            sql_ukm_jumlah = """
                SELECT status_ukm AS STATUS_UKM, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY STATUS_UKM ORDER BY JUMLAH_PAKET DESC
            """

            tabel_ukm_jumlah_trx = con.execute(sql_ukm_jumlah).df()

            grafik_ukm_1_1, grafik_ukm_1_2 = st.columns((3,7))

            with grafik_ukm_1_1:

                AgGrid(tabel_ukm_jumlah_trx)

            with grafik_ukm_1_2:

                figukmh = px.pie(tabel_ukm_jumlah_trx, values="JUMLAH_PAKET", names="STATUS_UKM", title='Grafik Status UKM - Jumlah Paket', hole=.3)
                st.plotly_chart(figukmh, theme="streamlit", use_container_width=True)

        with grafik_ukm_2:

            st.subheader("Berdasarkan Nilai Status UKM")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Status UKM

            sql_ukm_nilai = """
                SELECT status_ukm AS STATUS_UKM, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY STATUS_UKM ORDER BY NILAI_PAKET DESC
            """

            tabel_ukm_nilai_trx = con.execute(sql_ukm_nilai).df()

            grafik_ukm_2_1, grafik_ukm_2_2 = st.columns((3,7))

            with grafik_ukm_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_ukm_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_ukm_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)    
                
            with grafik_ukm_2_2:

                figukmn = px.pie(tabel_ukm_nilai_trx, values="NILAI_PAKET", names="STATUS_UKM", title='Grafik Status UKM - Nilai Paket', hole=.3)
                st.plotly_chart(figukmn, theme="streamlit", use_container_width=True)

        
        ####### Grafik jumlah dan nilai transaksi berdasarkan kualifikasi paket
        grafik_kp_1, grafik_kp_2 = st.tabs(["| Berdasarkan Jumlah Kualifikasi Paket |", "| Berdasarkan Nilai Kualifikasi Paket |"])

        with grafik_kp_1:

            st.subheader("Berdasarkan Jumlah Kualifikasi Paket")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan kualifikasi paket

            sql_kp_jumlah = """
                SELECT kualifikasi_paket AS KUALIFIKASI_PAKET, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY KUALIFIKASI_PAKET ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_kp_jumlah_trx = con.execute(sql_kp_jumlah).df()

            grafik_kp_1_1, grafik_kp_1_2 = st.columns((3,7))

            with grafik_kp_1_1:

                AgGrid(tabel_kp_jumlah_trx)

            with grafik_kp_1_2:

                st.bar_chart(tabel_kp_jumlah_trx, x="KUALIFIKASI_PAKET", y="JUMLAH_PAKET", color="KUALIFIKASI_PAKET")
    
        with grafik_kp_2:

            st.subheader("Berdasarkan Nilai Kualifikasi Paket")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan kualifikasi paket

            sql_kp_nilai = """
                SELECT kualifikasi_paket AS KUALIFIKASI_PAKET, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY KUALIFIKASI_PAKET ORDER BY NILAI_PAKET DESC
            """
            
            tabel_kp_nilai_trx = con.execute(sql_kp_nilai).df()

            grafik_kp_2_1, grafik_kp_2_2 = st.columns((3,7))

            with grafik_kp_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_kp_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_kp_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_kp_2_2:

                st.bar_chart(tabel_kp_nilai_trx, x="KUALIFIKASI_PAKET", y="NILAI_PAKET", color="KUALIFIKASI_PAKET")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan Jenis Pengadaan
        grafik_jp_1, grafik_jp_2 = st.tabs(["| Berdasarkan Jumlah Jenis Pengadaan |", "| Berdasarkan Nilai Jenis Pengadaan |"])

        with grafik_jp_1:

            st.subheader("Berdasarkan Jumlah Jenis Pengadaan")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Jenis Pengadaan

            sql_jp_jumlah = """
                SELECT jenis_pengadaan AS JENIS_PENGADAAN, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY JENIS_PENGADAAN ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_jp_jumlah_trx = con.execute(sql_jp_jumlah).df()

            grafik_jp_1_1, grafik_jp_1_2 = st.columns((3,7))

            with grafik_jp_1_1:

                AgGrid(tabel_jp_jumlah_trx)

            with grafik_jp_1_2:

                st.bar_chart(tabel_jp_jumlah_trx, x="JENIS_PENGADAAN", y="JUMLAH_PAKET", color="JENIS_PENGADAAN")
    
        with grafik_jp_2:

            st.subheader("Berdasarkan Nilai Jenis Pengadaan")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Jenis Pengadaan

            sql_jp_nilai = """
                SELECT jenis_pengadaan AS JENIS_PENGADAAN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY JENIS_PENGADAAN ORDER BY NILAI_PAKET DESC
            """
            
            tabel_jp_nilai_trx = con.execute(sql_jp_nilai).df()

            grafik_jp_2_1, grafik_jp_2_2 = st.columns((3,7))

            with grafik_jp_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_jp_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_jp_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_jp_2_2:

                st.bar_chart(tabel_jp_nilai_trx, x="JENIS_PENGADAAN", y="NILAI_PAKET", color="JENIS_PENGADAAN")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan Metode Pemilihan
        grafik_mp_1, grafik_mp_2 = st.tabs(["| Berdasarkan Jumlah Metode Pemilihan |", "| Berdasarkan Nilai Metode Pemilihan |"])

        with grafik_mp_1:

            st.subheader("Berdasarkan Jumlah Metode Pemilihan")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Metode Pemilihan

            sql_mp_jumlah = """
                SELECT mtd_pemilihan AS METODE_PEMILIHAN, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY METODE_PEMILIHAN ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_mp_jumlah_trx = con.execute(sql_mp_jumlah).df()

            grafik_mp_1_1, grafik_mp_1_2 = st.columns((3,7))

            with grafik_mp_1_1:

                AgGrid(tabel_mp_jumlah_trx)

            with grafik_mp_1_2:

                st.bar_chart(tabel_mp_jumlah_trx, x="METODE_PEMILIHAN", y="JUMLAH_PAKET", color="METODE_PEMILIHAN")
    
        with grafik_mp_2:

            st.subheader("Berdasarkan Nilai Metode Pemilihan")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Metode Pemilihan

            sql_mp_nilai = """
                SELECT mtd_pemilihan AS METODE_PEMILIHAN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY METODE_PEMILIHAN ORDER BY NILAI_PAKET DESC
            """
            
            tabel_mp_nilai_trx = con.execute(sql_mp_nilai).df()

            grafik_mp_2_1, grafik_mp_2_2 = st.columns((3,7))

            with grafik_mp_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_mp_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_mp_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_mp_2_2:

                st.bar_chart(tabel_mp_nilai_trx, x="METODE_PEMILIHAN", y="NILAI_PAKET", color="METODE_PEMILIHAN")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan Metode Evaluasi
        grafik_me_1, grafik_me_2 = st.tabs(["| Berdasarkan Jumlah Metode Evaluasi |", "| Berdasarkan Nilai Metode Evaluasi |"])

        with grafik_me_1:

            st.subheader("Berdasarkan Jumlah Metode Evaluasi")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Metode Evaluasi

            sql_me_jumlah = """
                SELECT mtd_evaluasi AS METODE_EVALUASI, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY METODE_EVALUASI ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_me_jumlah_trx = con.execute(sql_me_jumlah).df()

            grafik_me_1_1, grafik_me_1_2 = st.columns((3,7))

            with grafik_me_1_1:

                AgGrid(tabel_me_jumlah_trx)

            with grafik_me_1_2:

                st.bar_chart(tabel_me_jumlah_trx, x="METODE_EVALUASI", y="JUMLAH_PAKET", color="METODE_EVALUASI")
    
        with grafik_me_2:

            st.subheader("Berdasarkan Nilai Metode Evaluasi")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Metode Evaluasi

            sql_me_nilai = """
                SELECT mtd_evaluasi AS METODE_EVALUASI, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY METODE_EVALUASI ORDER BY NILAI_PAKET DESC
            """
            
            tabel_me_nilai_trx = con.execute(sql_me_nilai).df()

            grafik_me_2_1, grafik_me_2_2 = st.columns((3,7))

            with grafik_me_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_me_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_me_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_me_2_2:

                st.bar_chart(tabel_me_nilai_trx, x="METODE_EVALUASI", y="NILAI_PAKET", color="METODE_EVALUASI")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan Metode Kualifikasi
        grafik_mk_1, grafik_mk_2 = st.tabs(["| Berdasarkan Jumlah Metode Kualifikasi |", "| Berdasarkan Nilai Metode Kualifikasi |"])

        with grafik_mk_1:

            st.subheader("Berdasarkan Jumlah Metode Kualifikasi")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Metode Kualifikasi

            sql_mk_jumlah = """
                SELECT mtd_kualifikasi AS METODE_KUALIFIKASI, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY METODE_KUALIFIKASI ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_mk_jumlah_trx = con.execute(sql_mk_jumlah).df()

            grafik_mk_1_1, grafik_mk_1_2 = st.columns((3,7))

            with grafik_mk_1_1:

                AgGrid(tabel_mk_jumlah_trx)

            with grafik_mk_1_2:

                st.bar_chart(tabel_mk_jumlah_trx, x="METODE_KUALIFIKASI", y="JUMLAH_PAKET", color="METODE_KUALIFIKASI")
    
        with grafik_mk_2:

            st.subheader("Berdasarkan Nilai Metode Kualifikasi")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Metode Kualifikasi

            sql_mk_nilai = """
                SELECT mtd_kualifikasi AS METODE_KUALIFIKASI, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY METODE_KUALIFIKASI ORDER BY NILAI_PAKET DESC
            """
            
            tabel_mk_nilai_trx = con.execute(sql_mk_nilai).df()

            grafik_mk_2_1, grafik_mk_2_2 = st.columns((3,7))

            with grafik_mk_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_mk_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_mk_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_mk_2_2:

                st.bar_chart(tabel_mk_nilai_trx, x="METODE_KUALIFIKASI", y="NILAI_PAKET", color="METODE_KUALIFIKASI")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi berdasarkan Kontrak Pembayaran
        grafik_kontrak_1, grafik_kontrak_2 = st.tabs(["| Berdasarkan Jumlah Kontrak Pembayaran |", "| Berdasarkan Nilai Kontrak Pembayaran |"])

        with grafik_kontrak_1:

            st.subheader("Berdasarkan Jumlah Kontrak Pembayaran")

            #### Query data grafik jumlah transaksi pengumuman SPSE berdasarkan Kontrak Pembayaran

            sql_kontrak_jumlah = """
                SELECT kontrak_pembayaran AS KONTRAK_PEMBAYARAN, COUNT(DISTINCT(kd_tender)) AS JUMLAH_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY KONTRAK_PEMBAYARAN ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_kontrak_jumlah_trx = con.execute(sql_kontrak_jumlah).df()

            grafik_kontrak_1_1, grafik_kontrak_1_2 = st.columns((3,7))

            with grafik_kontrak_1_1:

                AgGrid(tabel_kontrak_jumlah_trx)

            with grafik_kontrak_1_2:

                st.bar_chart(tabel_kontrak_jumlah_trx, x="KONTRAK_PEMBAYARAN", y="JUMLAH_PAKET", color="KONTRAK_PEMBAYARAN")
    
        with grafik_kontrak_2:

            st.subheader("Berdasarkan Nilai Kontrak Pembayaran")

            #### Query data grafik nilai transaksi pengumuman SPSE berdasarkan Kontrak Pembayaran

            sql_kontrak_nilai = """
                SELECT kontrak_pembayaran AS KONTRAK_PEMBAYARAN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSETenderPengumuman_filter GROUP BY KONTRAK_PEMBAYARAN ORDER BY NILAI_PAKET DESC
            """
            
            tabel_kontrak_nilai_trx = con.execute(sql_kontrak_nilai).df()

            grafik_kontrak_2_1, grafik_kontrak_2_2 = st.columns((3,7))

            with grafik_kontrak_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_kontrak_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_kontrak_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_kontrak_2_2:

                st.bar_chart(tabel_kontrak_nilai_trx, x="KONTRAK_PEMBAYARAN", y="NILAI_PAKET", color="KONTRAK_PEMBAYARAN")

        st.divider()

    #### Tab menu SPSE - Tender - SPPBJ
    with menu_spse_1_2:

        ##### Buat tombol unduh dataset SPSE-Tender-SPPBJ
        unduh_SPSE_Tender_SPPBJ = unduh_data(df_SPSETenderSPPBJ)

        SPSE_SPPBJ_1, SPSE_SPPBJ_2 = st.columns((7,3))
        with SPSE_SPPBJ_1:
            st.subheader("SPSE-Tender-SPPBJ")
        with SPSE_SPPBJ_2:
            st.download_button(
                label = "📥 Download Data Tender SPPBJ",
                data = unduh_SPSE_Tender_SPPBJ,
                file_name = f"SPSETenderSPPBJ-{kodeFolder}-{tahun}.csv",
                mime = "text/csv"
            )

        st.divider()

        SPSE_SPPBJ_radio_1, SPSE_SPPBJ_radio_2 = st.columns((2,8))
        with SPSE_SPPBJ_radio_1:
            status_kontrak_TSPPBJ = st.radio("**Status Kontrak**", df_SPSETenderSPPBJ['status_kontrak'].unique(), key='Tender_Status_SPPBJ')
        with SPSE_SPPBJ_radio_2:
            opd_TSPPBJ = st.selectbox("Pilih Perangkat Daerah :", df_SPSETenderSPPBJ['nama_satker'].unique(), key='Tender_OPD_SPPBJ')
        st.write(f"Anda memilih : **{status_kontrak_TSPPBJ}** dari **{opd_TSPPBJ}**")

        ##### Hitung-hitungan dataset SPSE-Tender-SPPBJ
        df_SPSETenderSPPBJ_filter = con.execute(f"SELECT * FROM df_SPSETenderSPPBJ WHERE status_kontrak = '{status_kontrak_TSPPBJ}' AND nama_satker = '{opd_TSPPBJ}'").df()
        jumlah_trx_spse_sppbj = df_SPSETenderSPPBJ_filter['kd_tender'].unique().shape[0]
        nilai_trx_spse_sppbj_final = df_SPSETenderSPPBJ_filter['harga_final'].sum()

        data_sppbj_1, data_sppbj_2 = st.columns(2)
        data_sppbj_1.metric(label="Jumlah Tender SPPBJ", value="{:,}".format(jumlah_trx_spse_sppbj))
        data_sppbj_2.metric(label="Nilai Tender SPPBJ", value="{:,.2f}".format(nilai_trx_spse_sppbj_final))
        style_metric_cards()

        st.divider()
        
        sql_tender_sppbj_trx = """
            SELECT nama_paket AS NAMA_PAKET, no_sppbj AS NO_SPPBJ, tgl_sppbj AS TGL_SPPBJ, 
            nama_ppk AS NAMA_PPK, nama_penyedia AS NAMA_PENYEDIA, npwp_penyedia AS NPWP_PENYEDIA, 
            harga_final AS HARGA_FINAL FROM df_SPSETenderSPPBJ_filter
        """
        tabel_tender_sppbj_tampil = con.execute(sql_tender_sppbj_trx).df()

        ##### Tampilkan data SPSE Tender SPPBJ menggunakan AgGrid
        gd = GridOptionsBuilder.from_dataframe(tabel_tender_sppbj_tampil)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("HARGA_FINAL", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.HARGA_FINAL.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

        gridOptions = gd.build()
        AgGrid(tabel_tender_sppbj_tampil, gridOptions=gridOptions, enable_enterprise_modules=True) 

    #### Tab menu SPSE - Tender - Kontrak
    with menu_spse_1_3:

        ##### Buat tombol unduh dataset SPSE-Tender-Kontrak
        unduh_SPSE_Tender_KONTRAK = unduh_data(df_SPSETenderKontrak)

        SPSE_KONTRAK_1, SPSE_KONTRAK_2 = st.columns((7,3))
        with SPSE_KONTRAK_1:
            st.subheader("SPSE-Tender-KONTRAK")
        with SPSE_KONTRAK_2:
            st.download_button(
                label = "📥 Download Data Tender Kontrak",
                data = unduh_SPSE_Tender_KONTRAK,
                file_name = f"SPSETenderKontrak-{kodeFolder}-{tahun}.csv",
                mime = "txt/csv"
            )

        st.divider()

        SPSE_KONTRAK_radio_1, SPSE_KONTRAK_radio_2 = st.columns((2,8))
        with SPSE_KONTRAK_radio_1:
            status_kontrak_TKONTRAK = st.radio("**Status Kontrak**", df_SPSETenderKontrak['status_kontrak'].unique(), key='Tender_Status_Kontrak')
        with SPSE_KONTRAK_radio_2:
            opd_TKONTRAK = st.selectbox("Pilih Perangkat Daerah :", df_SPSETenderKontrak['nama_satker'].unique(), key='Tender_OPD_Kontrak')
        st.write(f"Anda memilih : **{status_kontrak_TKONTRAK}** dari **{opd_TKONTRAK}**")

        ##### Hitung-hitungan dataset SPSE-Tender-Kontrak
        df_SPSETenderKontrak_filter = con.execute(f"SELECT * FROM df_SPSETenderKontrak WHERE status_kontrak = '{status_kontrak_TKONTRAK}' AND nama_satker = '{opd_TKONTRAK}'").df()
        jumlah_trx_spse_kontrak = df_SPSETenderKontrak_filter['kd_tender'].unique().shape[0]
        nilai_trx_spse_kontrak_nilaikontrak = df_SPSETenderKontrak_filter['nilai_kontrak'].sum()

        data_kontrak_1, data_kontrak_2 = st.columns(2)
        data_kontrak_1.metric(label="Jumlah Tender Berkontrak", value="{:,}".format(jumlah_trx_spse_kontrak))
        data_kontrak_2.metric(label="Nilai Tender Berkontrak", value="{:,.2f}".format(nilai_trx_spse_kontrak_nilaikontrak))
        style_metric_cards()

        st.divider()

        sql_tender_kontrak_trx = """
            SELECT nama_paket AS NAMA_PAKET, no_kontrak AS NO_KONTRAK, tgl_kontrak AS TGL_KONTRAK,
            nama_ppk AS NAMA_PPK, nama_penyedia AS NAMA_PENYEDIA, wakil_sah_penyedia AS WAKIL_SAH,
            npwp_penyedia AS NPWP_PENYEDIA, nilai_kontrak AS NILAI_KONTRAK, nilai_pdn_kontrak AS NILAI_PDN, nilai_umk_kontrak AS NILAI_UMK
            FROM df_SPSETenderKontrak_filter 
        """
        tabel_tender_kontrak_tampil = con.execute(sql_tender_kontrak_trx).df()

        ##### Tampilkan data SPSE Tender Kontrak menggunakan AgGrid
        gd = GridOptionsBuilder.from_dataframe(tabel_tender_kontrak_tampil)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_KONTRAK", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_KONTRAK.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("NILAI_PDN", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PDN.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("NILAI_UMK", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_UMK.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

        gridOptions = gd.build()
        AgGrid(tabel_tender_kontrak_tampil, gridOptions=gridOptions, enable_enterprise_modules=True)

    #### Tab menu SPSE - Tender - SPMK
    with menu_spse_1_4:

        ##### Buat tombol unduh dataset SPSE-Tender-Kontrak
        df_SPSETenderKontrak_filter_kolom = df_SPSETenderKontrak[["kd_tender", "nilai_kontrak", "nilai_pdn_kontrak", "nilai_umk_kontrak"]]
        df_SPSETenderSPMK_OK = df_SPSETenderSPMK.merge(df_SPSETenderKontrak_filter_kolom, how='left', on='kd_tender')
        unduh_SPSE_Tender_SPMK = unduh_data(df_SPSETenderSPMK_OK)

        SPSE_SPMK_1, SPSE_SPMK_2 = st.columns((7,3))
        with SPSE_SPMK_1:
            st.subheader("SPSE-Tender-SPMK")
        with SPSE_SPMK_2:
            st.download_button(
                label = "📥 Download Data Tender SPMK",
                data = unduh_SPSE_Tender_SPMK,
                file_name = f"SPSETenderSPMK-{kodeFolder}-{tahun}.csv",
                mime = "txt/csv"
            )

        st.divider()
        
        opd_TSPMK = st.selectbox("Pilih Perangkat Daerah :", df_SPSETenderSPMK_OK['nama_satker'].unique(), key='Tender_OPD_SPMK')
        st.write(f"Anda memilih : **{opd_TSPMK}**")

        ##### Hitung-hitungan dataset SPSE-Tender-Kontrak
        df_SPSETenderSPMK_filter = con.execute(f"SELECT * FROM df_SPSETenderSPMK_OK WHERE nama_satker = '{opd_TSPMK}'").df()
        jumlah_trx_spse_spmk = df_SPSETenderSPMK_filter['kd_tender'].unique().shape[0]
        nilai_trx_spse_spmk_nilaikontrak = df_SPSETenderSPMK_filter['nilai_kontrak'].sum()

        data_spmk_1, data_spmk_2 = st.columns(2)
        data_spmk_1.metric(label="Jumlah Tender SPMK", value="{:,}".format(jumlah_trx_spse_spmk))
        data_spmk_2.metric(label="Nilai Tender SPMK", value="{:,.2f}".format(nilai_trx_spse_spmk_nilaikontrak))
        style_metric_cards()

        st.divider()

        sql_tender_spmk_trx = """
            SELECT nama_paket AS NAMA_PAKET, no_spmk_spp AS NO_SPMK, tgl_spmk_spp AS TGL_SPMK,
            nama_ppk AS NAMA_PPK, nama_penyedia AS NAMA_PENYEDIA, wakil_sah_penyedia AS WAKIL_SAH,
            npwp_penyedia AS NPWP_PENYEDIA, nilai_kontrak AS NILAI_KONTRAK, nilai_pdn_kontrak AS NILAI_PDN, nilai_umk_kontrak AS NILAI_UMK
            FROM df_SPSETenderSPMK_filter 
        """
        tabel_tender_spmk_tampil = con.execute(sql_tender_spmk_trx).df()
        
        ##### Tampilkan data SPSE Tender Kontrak menggunakan AgGrid
        gd = GridOptionsBuilder.from_dataframe(tabel_tender_spmk_tampil)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("NILAI_KONTRAK", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_KONTRAK.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("NILAI_PDN", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PDN.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("NILAI_UMK", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_UMK.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

        gridOptions = gd.build()
        AgGrid(tabel_tender_spmk_tampil, gridOptions=gridOptions, enable_enterprise_modules=True)

    #### Tab menu SPSE - Tender - BAPBAST
    with menu_spse_1_5:

        st.subheader("SPSE-Tender-BAPBAST")

## Tab menu SPSE - Non Tender
with menu_spse_2:

    st.header(f"SPSE - Non Tender - {pilih}")

    ### Buat dataset gabung df_SPSENonTenderPengumuman + df_RUPPP_umumkan_filter
    df_SPSENonTenderPengumuman_OK = df_SPSENonTenderPengumuman

    ### Buat sub menu SPSE - Non Tender
    menu_spse_2_1, menu_spse_2_2, menu_spse_2_3, menu_spse_2_4, menu_spse_2_5, menu_spse_2_6 = st.tabs(["| PENGUMUMAN |", "| SELESAI |", "| SPPBJ |", "| KONTRAK |", "| SPMK |", "| BAPBAST |"])

    #### Tab menu SPSE - Non Tender - Pengumuman
    with menu_spse_2_1:

        ##### Buat tombol unduh dataset SPSE-NonTender-Pengumuman
        unduh_SPSE_NT_Pengumuman = unduh_data(df_SPSENonTenderPengumuman_OK)

        SPSE_NT_Umumkan_1, SPSE_NT_Umumkan_2 = st.columns((7,3))
        with SPSE_NT_Umumkan_1:
            st.subheader("SPSE - Non Tender - Pengumuman")
        with SPSE_NT_Umumkan_2:
            st.download_button(
                label = "📥 Download Data Pengumuman Non Tender",
                data = unduh_SPSE_NT_Pengumuman,
                file_name = f"SPSENonTenderPengumuman-{kodeFolder}-{tahun}.csv",
                mime = "text/csv"
            )

        st.divider()

        SPSE_NT_radio_1, SPSE_NT_radio_2, SPSE_NT_radio_3 = st.columns((1,1,8))
        with SPSE_NT_radio_1:
            sumber_dana_nt = st.radio("**Sumber Dana**", df_SPSENonTenderPengumuman_OK['sumber_dana'].unique())
        with SPSE_NT_radio_2:
            status_nontender = st.radio("**Status Non Tender**", df_SPSENonTenderPengumuman_OK['status_nontender'].unique())
        st.write(f"Anda memilih : **{sumber_dana_nt}** dan **{status_nontender}**")

        ##### Hitung-hitungan dataset Non Tender Pengumuman
        df_SPSENonTenderPengumuman_filter = con.execute(f"SELECT kd_nontender, pagu, hps, kualifikasi_paket, jenis_pengadaan, mtd_pemilihan, kontrak_pembayaran FROM df_SPSENonTenderPengumuman_OK WHERE sumber_dana = '{sumber_dana_nt}' AND status_nontender = '{status_nontender}'").df()
        jumlah_trx_spse_nt_pengumuman = df_SPSENonTenderPengumuman_filter['kd_nontender'].unique().shape[0]
        nilai_trx_spse_nt_pengumuman_pagu = df_SPSENonTenderPengumuman_filter['pagu'].sum()
        nilai_trx_spse_nt_pengumuman_hps = df_SPSENonTenderPengumuman_filter['hps'].sum()

        data_umum_nt_1, data_umum_nt_2, data_umum_nt_3 = st.columns(3)
        data_umum_nt_1.metric(label="Jumlah Non Tender Diumumkan", value="{:,}".format(jumlah_trx_spse_nt_pengumuman))
        data_umum_nt_2.metric(label="Nilai Pagu Non Tender Diumumkan", value="{:,}".format(nilai_trx_spse_nt_pengumuman_pagu))
        data_umum_nt_3.metric(label="Nilai HPS Non Tender Diumumkan", value="{:,}".format(nilai_trx_spse_nt_pengumuman_hps))
        style_metric_cards()

        st.divider()

        ####### Grafik jumlah dan nilai transaksi Non Tender berdasarkan kualifikasi paket
        grafik_kp_nt_1, grafik_kp_nt_2 = st.tabs(["| Berdasarkan Jumlah Kualifikasi Paket |", "| Berdasarkan Nilai Kualifikasi Paket |"])

        with grafik_kp_nt_1:

            st.subheader("Berdasarkan Jumlah Kualifikasi Paket (Non Tender)")

            #### Query data grafik jumlah transaksi pengumuman SPSE Non Tender berdasarkan kualifikasi paket

            sql_kp_nt_jumlah = """
                SELECT kualifikasi_paket AS KUALIFIKASI_PAKET, COUNT(DISTINCT(kd_nontender)) AS JUMLAH_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY KUALIFIKASI_PAKET ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_kp_nt_jumlah_trx = con.execute(sql_kp_nt_jumlah).df()

            grafik_kp_nt_1_1, grafik_kp_nt_1_2 = st.columns((3,7))

            with grafik_kp_nt_1_1:

                AgGrid(tabel_kp_nt_jumlah_trx)

            with grafik_kp_nt_1_2:

                st.bar_chart(tabel_kp_nt_jumlah_trx, x="KUALIFIKASI_PAKET", y="JUMLAH_PAKET", color="KUALIFIKASI_PAKET")
    
        with grafik_kp_nt_2:

            st.subheader("Berdasarkan Nilai Kualifikasi Paket (Non Tender)")

            #### Query data grafik nilai transaksi pengumuman SPSE Non Tender berdasarkan kualifikasi paket

            sql_kp_nt_nilai = """
                SELECT kualifikasi_paket AS KUALIFIKASI_PAKET, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY KUALIFIKASI_PAKET ORDER BY NILAI_PAKET DESC
            """
            
            tabel_kp_nt_nilai_trx = con.execute(sql_kp_nt_nilai).df()

            grafik_kp_nt_2_1, grafik_kp_nt_2_2 = st.columns((3,7))

            with grafik_kp_nt_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_kp_nt_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_kp_nt_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_kp_nt_2_2:

                st.bar_chart(tabel_kp_nt_nilai_trx, x="KUALIFIKASI_PAKET", y="NILAI_PAKET", color="KUALIFIKASI_PAKET")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi Non Tender berdasarkan Jenis Pengadaan
        grafik_jp_nt_1, grafik_jp_nt_2 = st.tabs(["| Berdasarkan Jumlah Jenis Pengadaan |", "| Berdasarkan Nilai Jenis Pengadaan |"])

        with grafik_jp_nt_1:

            st.subheader("Berdasarkan Jumlah Jenis Pengadaan (Non Tender)")

            #### Query data grafik jumlah transaksi pengumuman SPSE Non Tender berdasarkan Jenis Pengadaan

            sql_jp_nt_jumlah = """
                SELECT jenis_pengadaan AS JENIS_PENGADAAN, COUNT(DISTINCT(kd_nontender)) AS JUMLAH_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY JENIS_PENGADAAN ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_jp_nt_jumlah_trx = con.execute(sql_jp_nt_jumlah).df()

            grafik_jp_nt_1_1, grafik_jp_nt_1_2 = st.columns((3,7))

            with grafik_jp_nt_1_1:

                AgGrid(tabel_jp_nt_jumlah_trx)

            with grafik_jp_nt_1_2:

                st.bar_chart(tabel_jp_nt_jumlah_trx, x="JENIS_PENGADAAN", y="JUMLAH_PAKET", color="JENIS_PENGADAAN")
    
        with grafik_jp_nt_2:

            st.subheader("Berdasarkan Nilai Jenis Pengadaan (Non Tender)")

            #### Query data grafik nilai transaksi pengumuman SPSE Non Tender berdasarkan Jenis Pengadaan

            sql_jp_nt_nilai = """
                SELECT jenis_pengadaan AS JENIS_PENGADAAN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY JENIS_PENGADAAN ORDER BY NILAI_PAKET DESC
            """
            
            tabel_jp_nt_nilai_trx = con.execute(sql_jp_nt_nilai).df()

            grafik_jp_nt_2_1, grafik_jp_nt_2_2 = st.columns((3,7))

            with grafik_jp_nt_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_jp_nt_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_jp_nt_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_jp_nt_2_2:

                st.bar_chart(tabel_jp_nt_nilai_trx, x="JENIS_PENGADAAN", y="NILAI_PAKET", color="JENIS_PENGADAAN")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi Non Tender berdasarkan Metode Pemilihan
        grafik_mp_nt_1, grafik_mp_nt_2 = st.tabs(["| Berdasarkan Jumlah Metode Pemilihan |", "| Berdasarkan Nilai Metode Pemilihan |"])

        with grafik_mp_nt_1:

            st.subheader("Berdasarkan Jumlah Metode Pemilihan (Non Tender)")

            #### Query data grafik jumlah transaksi pengumuman SPSE Non Tender berdasarkan Metode Pemilihan

            sql_mp_nt_jumlah = """
                SELECT mtd_pemilihan AS METODE_PEMILIHAN, COUNT(DISTINCT(kd_nontender)) AS JUMLAH_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY METODE_PEMILIHAN ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_mp_nt_jumlah_trx = con.execute(sql_mp_nt_jumlah).df()

            grafik_mp_nt_1_1, grafik_mp_nt_1_2 = st.columns((3,7))

            with grafik_mp_nt_1_1:

                AgGrid(tabel_mp_nt_jumlah_trx)

            with grafik_mp_nt_1_2:

                st.bar_chart(tabel_mp_nt_jumlah_trx, x="METODE_PEMILIHAN", y="JUMLAH_PAKET", color="METODE_PEMILIHAN")
    
        with grafik_mp_nt_2:

            st.subheader("Berdasarkan Nilai Metode Pemilihan (Non Tender)")

            #### Query data grafik nilai transaksi pengumuman SPSE Non Tender berdasarkan Metode Pemilihan

            sql_mp_nt_nilai = """
                SELECT mtd_pemilihan AS METODE_PEMILIHAN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY METODE_PEMILIHAN ORDER BY NILAI_PAKET DESC
            """
            
            tabel_mp_nt_nilai_trx = con.execute(sql_mp_nt_nilai).df()

            grafik_mp_nt_2_1, grafik_mp_nt_2_2 = st.columns((3,7))

            with grafik_mp_nt_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_mp_nt_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_mp_nt_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_mp_nt_2_2:

                st.bar_chart(tabel_mp_nt_nilai_trx, x="METODE_PEMILIHAN", y="NILAI_PAKET", color="METODE_PEMILIHAN")

        st.divider()

        ####### Grafik jumlah dan nilai transaksi Non Tender berdasarkan Kontrak Pembayaran
        grafik_kontrak_nt_1, grafik_kontrak_nt_2 = st.tabs(["| Berdasarkan Jumlah Kontrak Pembayaran |", "| Berdasarkan Nilai Kontrak Pembayaran |"])

        with grafik_kontrak_nt_1:

            st.subheader("Berdasarkan Jumlah Kontrak Pembayaran (Non Tender)")

            #### Query data grafik jumlah transaksi pengumuman SPSE Non Tender berdasarkan Kontrak Pembayaran

            sql_kontrak_nt_jumlah = """
                SELECT kontrak_pembayaran AS KONTRAK_PEMBAYARAN, COUNT(DISTINCT(kd_nontender)) AS JUMLAH_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY KONTRAK_PEMBAYARAN ORDER BY JUMLAH_PAKET DESC
            """
            
            tabel_kontrak_nt_jumlah_trx = con.execute(sql_kontrak_nt_jumlah).df()

            grafik_kontrak_nt_1_1, grafik_kontrak_nt_1_2 = st.columns((3,7))

            with grafik_kontrak_nt_1_1:

                AgGrid(tabel_kontrak_nt_jumlah_trx)

            with grafik_kontrak_nt_1_2:

                st.bar_chart(tabel_kontrak_nt_jumlah_trx, x="KONTRAK_PEMBAYARAN", y="JUMLAH_PAKET", color="KONTRAK_PEMBAYARAN")
    
        with grafik_kontrak_nt_2:

            st.subheader("Berdasarkan Nilai Kontrak Pembayaran (Non Tender)")

            #### Query data grafik nilai transaksi pengumuman SPSE Non Tender berdasarkan Kontrak Pembayaran

            sql_kontrak_nt_nilai = """
                SELECT kontrak_pembayaran AS KONTRAK_PEMBAYARAN, SUM(pagu) AS NILAI_PAKET
                FROM df_SPSENonTenderPengumuman_filter GROUP BY KONTRAK_PEMBAYARAN ORDER BY NILAI_PAKET DESC
            """
            
            tabel_kontrak_nt_nilai_trx = con.execute(sql_kontrak_nt_nilai).df()

            grafik_kontrak_nt_2_1, grafik_kontrak_nt_2_2 = st.columns((3,7))

            with grafik_kontrak_nt_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_kontrak_nt_nilai_trx)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_PAKET", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PAKET.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_kontrak_nt_nilai_trx, gridOptions=gridOptions, enable_enterprise_modules=True)

            with grafik_kontrak_nt_2_2:

                st.bar_chart(tabel_kontrak_nt_nilai_trx, x="KONTRAK_PEMBAYARAN", y="NILAI_PAKET", color="KONTRAK_PEMBAYARAN")

    #### Tab menu SPSE - Non Tender - Selesai
    with menu_spse_2_2:

        st.subheader("SPSE - Non Tender - Selesai")

    #### Tab menu SPSE - Non Tender - SPPBJ
    with menu_spse_2_3:

        ##### Buat tombol unduh dataset SPSE-Tender-SPPBJ
        unduh_SPSE_NonTender_SPPBJ = unduh_data(df_SPSENonTenderSPPBJ)

        SPSE_SPPBJ_NT_1, SPSE_SPPBJ_NT_2 = st.columns((7,3))
        with SPSE_SPPBJ_NT_1:
            st.subheader("SPSE - Non Tender - SPPBJ")
        with SPSE_SPPBJ_NT_2:
            st.download_button(
                label = "📥 Download Data Non Tender SPPBJ",
                data = unduh_SPSE_Tender_SPPBJ,
                file_name = f"SPSENonTenderSPPBJ-{kodeFolder}-{tahun}.csv",
                mime = "text/csv"
            )

        st.divider()

        SPSE_SPPBJ_NT_radio_1, SPSE_SPPBJ_NT_radio_2 = st.columns((2,8))
        with SPSE_SPPBJ_NT_radio_1:
            status_kontrak_nt = st.radio("**Status Kontrak**", df_SPSENonTenderSPPBJ['status_kontrak'].unique())
        with SPSE_SPPBJ_NT_radio_2:
            opd_nt = st.selectbox("Pilih Perangkat Daerah :", df_SPSENonTenderSPPBJ['nama_satker'].unique())
        st.write(f"Anda memilih : **{status_kontrak_nt}** dari **{opd_nt}**")

        ##### Hitung-hitungan dataset SPSE-Tender-SPPBJ
        df_SPSENonTenderSPPBJ_filter = con.execute(f"SELECT * FROM df_SPSENonTenderSPPBJ WHERE status_kontrak = '{status_kontrak_nt}' AND nama_satker = '{opd_nt}'").df()
        jumlah_trx_spse_nt_sppbj = df_SPSENonTenderSPPBJ_filter['kd_nontender'].unique().shape[0]
        nilai_trx_spse_nt_sppbj_final = df_SPSENonTenderSPPBJ_filter['harga_final'].sum()

        data_sppbj_nt_1, data_sppbj_nt_2 = st.columns(2)
        data_sppbj_nt_1.metric(label="Jumlah Non Tender SPPBJ", value="{:,}".format(jumlah_trx_spse_nt_sppbj))
        data_sppbj_nt_2.metric(label="Nilai Non Tender SPPBJ", value="{:,.2f}".format(nilai_trx_spse_nt_sppbj_final))
        style_metric_cards()

        st.divider()
        
        sql_sppbj_nt_trx = """
            SELECT nama_paket AS NAMA_PAKET, no_sppbj AS NO_SPPBJ, tgl_sppbj AS TGL_SPPBJ, 
            nama_ppk AS NAMA_PPK, nama_penyedia AS NAMA_PENYEDIA, npwp_penyedia AS NPWP_PENYEDIA, 
            harga_final AS HARGA_FINAL FROM df_SPSENonTenderSPPBJ_filter
        """
        tabel_sppbj_nt_tampil = con.execute(sql_sppbj_nt_trx).df()

        ##### Tampilkan data SPSE Tender SPPBJ menggunakan AgGrid
        gd = GridOptionsBuilder.from_dataframe(tabel_sppbj_nt_tampil)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("HARGA_FINAL", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.HARGA_FINAL.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")

        gridOptions = gd.build()
        AgGrid(tabel_sppbj_nt_tampil, gridOptions=gridOptions, enable_enterprise_modules=True) 


    #### Tab menu SPSE - Non Tender - Kontrak
    with menu_spse_2_4:

        st.subheader("SPSE - Non Tender - Kontrak")

    #### Tab menu SPSE - Non Tender - SPMK
    with menu_spse_2_5:

        st.subheader("SPSE - Non Tender - SPMK")

    #### Tab menu SPSE - Non Tender - BABBAST
    with menu_spse_2_6:

        st.subheader("SPSE - Non Tender - BAPBAST")


## Tab menu SPSE - Pencatatan
with menu_spse_3:

    st.header(f"SPSE - Pencatatan Transaksi PBJ - {pilih}")

    ### Buat sub menu SPSE - Pencatatan Transaksi PBJ
    menu_spse_3_1, menu_spse_3_2 = st.tabs(["| Pencatatan Non Tender |", "| Pencatatan Swakelola |"])

    #### Query penggabungan dataset CatatNonTender dan CatatSwakelola
    df_CatatNonTenderRealisasi_filter = df_CatatNonTenderRealisasi[["kd_nontender_pct", "jenis_realisasi", "no_realisasi", "tgl_realisasi", "nilai_realisasi", "nama_penyedia", "npwp_penyedia"]]
    df_CatatNonTender_OK = df_CatatNonTender.merge(df_CatatNonTenderRealisasi_filter, how='left', on='kd_nontender_pct')

    df_CatatSwakelolaRealisasi_filter = df_CatatSwakelolaRealisasi[["kd_swakelola_pct", "jenis_realisasi", "no_realisasi", "tgl_realisasi", "nilai_realisasi"]] 
    df_CatatSwakelola_OK = df_CatatSwakelola.merge(df_CatatSwakelolaRealisasi_filter, how='left', on='kd_swakelola_pct')
    
    #### Tab menu SPSE - Pencatatan - Non Tender
    with menu_spse_3_1:

        #### Buat tombol unduh dataset SPSE-Pencatatan-Non Tender
        unduh_CATAT_NonTender = unduh_data(df_CatatNonTender_OK)

        SPSE_CATAT_NonTender_1, SPSE_CATAT_NonTender_2 = st.columns((7,3))
        with SPSE_CATAT_NonTender_1:
            st.subheader("Pencatatan Non Tender")
        with SPSE_CATAT_NonTender_2:
            st.download_button(
                label = "📥 Download Data Pencatatan Non Tender",
                data = unduh_CATAT_NonTender,
                file_name = f"SPSEPencatatanNonTender-{kodeFolder}-{tahun}.csv",
                mime = "text/csv"
            )

        st.divider()

        sumber_dana_cnt = st.radio("**Sumber Dana :**", df_CatatNonTender_OK['sumber_dana'].unique(), key="CatatNonTender")
        st.write(f"Anda memilih : **{sumber_dana_cnt}**")

        #### Hitung-hitungan dataset Catat Non Tender
        df_CatatNonTender_OK_filter = df_CatatNonTender_OK.query(f"sumber_dana == '{sumber_dana_cnt}'")
        jumlah_CatatNonTender_Berjalan = df_CatatNonTender_OK_filter.query("status_nontender_pct_ket == 'Paket Sedang Berjalan'")
        jumlah_CatatNonTender_Selesai = df_CatatNonTender_OK_filter.query("status_nontender_pct_ket == 'Paket Selesai'")
        jumlah_CatatNonTender_Dibatalkan = df_CatatNonTender_OK_filter.query("status_nontender_pct_ket == 'Paket Dibatalkan'")

        data_cnt_1, data_cnt_2, data_cnt_3 = st.columns(3)
        data_cnt_1.metric(label="Jumlah Pencatatan NonTender Berjalan", value="{:,}".format(jumlah_CatatNonTender_Berjalan.shape[0]))
        data_cnt_2.metric(label="Jumlah Pencatatan NonTender Selesai", value="{:,}".format(jumlah_CatatNonTender_Selesai.shape[0]))
        data_cnt_3.metric(label="Jumlah Pencatatan NonTender Dibatalkan", value="{:,}".format(jumlah_CatatNonTender_Dibatalkan.shape[0]))
        style_metric_cards()

        st.divider()
        
        #### Grafik jumlah dan nilai transaksi berdasarkan kategori pengadaan dan metode pemilihan
        grafik_cnt_1, grafik_cnt_2, grafik_cnt_3, grafik_cnt_4 = st.tabs(["| Jumlah Transaksi - Kategori Pengadaan |","| Nilai Transaksi - Kategori Pengadaan |","| Jumlah Transaksi - Metode Pemilihan |","| Nilai Transaksi - Metode Pemilihan |"])
        
        with grafik_cnt_1:

            st.subheader("Berdasarkan Jumlah Kategori Pemilihan")

            ##### Query data grafik jumlah transaksi Pencatatan Non Tender berdasarkan Kategori Pengadaan

            sql_cnt_kp_jumlah = """
                SELECT kategori_pengadaan AS KATEGORI_PENGADAAN, COUNT(kd_nontender_pct) AS JUMLAH_PAKET
                FROM df_CatatNonTender_OK_filter GROUP BY KATEGORI_PENGADAAN ORDER BY JUMLAH_PAKET DESC
            """

            tabel_cnt_kp_jumlah = con.execute(sql_cnt_kp_jumlah).df()

            grafik_cnt_1_1, grafik_cnt_1_2 = st.columns((3,7))

            with grafik_cnt_1_1:

                AgGrid(tabel_cnt_kp_jumlah)

            with grafik_cnt_1_2:

                figcntkph = px.pie(tabel_cnt_kp_jumlah, values="JUMLAH_PAKET", names="KATEGORI_PENGADAAN", title="Grafik Pencatatan Non Tender - Jumlah Paket - Kategori Pengadaan", hole=.3)
                st.plotly_chart(figcntkph, theme="streamlit", use_container_width=True)

        with grafik_cnt_2:

            st.subheader("Berdasarkan Nilai Kategori Pemilihan")

            ##### Query data grafik nilai transaksi Pencatatan Non Tender berdasarkan Kategori Pengadaan

            sql_cnt_kp_nilai = """
                SELECT kategori_pengadaan AS KATEGORI_PENGADAAN, SUM(nilai_realisasi) AS NILAI_REALISASI
                FROM df_CatatNonTender_OK_filter GROUP BY KATEGORI_PENGADAAN ORDER BY NILAI_REALISASI
            """

            tabel_cnt_kp_nilai = con.execute(sql_cnt_kp_nilai).df()

            grafik_cnt_2_1, grafik_cnt_2_2 = st.columns((3,7))

            with grafik_cnt_2_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_cnt_kp_nilai)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_REALISASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_REALISASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_cnt_kp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)    

            with grafik_cnt_2_2:

                figcntkpn = px.pie(tabel_cnt_kp_nilai, values="NILAI_REALISASI", names="KATEGORI_PENGADAAN", title="Grafik Pencatatan Non Tender - Nilai Transaksi - Kategori Pengadaan", hole=.3)
                st.plotly_chart(figcntkpn, theme="streamlit", use_container_width=True)

        with grafik_cnt_3:

            st.subheader("Berdasarkan Jumlah Metode Pemilihan")

            ##### Query data grafik jumlah transaksi Pencatatan Non Tender berdasarkan Metode Pemilihan

            sql_cnt_mp_jumlah = """
                SELECT mtd_pemilihan AS METODE_PEMILIHAN, COUNT(kd_nontender_pct) AS JUMLAH_PAKET
                FROM df_CatatNonTender_OK_filter GROUP BY METODE_PEMILIHAN ORDER BY JUMLAH_PAKET DESC
            """

            tabel_cnt_mp_jumlah = con.execute(sql_cnt_mp_jumlah).df()

            grafik_cnt_3_1, grafik_cnt_3_2 = st.columns((3,7))

            with grafik_cnt_3_1:

                AgGrid(tabel_cnt_mp_jumlah)

            with grafik_cnt_3_2:

                figcntmph = px.pie(tabel_cnt_mp_jumlah, values="JUMLAH_PAKET", names="METODE_PEMILIHAN", title="Grafik Pencatatan Non Tender - Jumlah Paket - Metode Pemilihan", hole=.3)
                st.plotly_chart(figcntmph, theme="streamlit", use_container_width=True)


        with grafik_cnt_4:

            st.subheader("Berdasarkan Nilai Metode Pemilihan")

            ##### Query data grafik nilai transaksi Pencatatan Non Tender berdasarkan Metode Pemilihan

            sql_cnt_mp_nilai = """
                SELECT mtd_pemilihan AS METODE_PEMILIHAN, SUM(nilai_realisasi) AS NILAI_REALISASI
                FROM df_CatatNonTender_OK_filter GROUP BY METODE_PEMILIHAN ORDER BY NILAI_REALISASI
            """

            tabel_cnt_mp_nilai = con.execute(sql_cnt_mp_nilai).df()

            grafik_cnt_4_1, grafik_cnt_4_2 = st.columns((3,7))

            with grafik_cnt_4_1:

                gd = GridOptionsBuilder.from_dataframe(tabel_cnt_mp_nilai)
                gd.configure_pagination()
                gd.configure_side_bar()
                gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
                gd.configure_column("NILAI_REALISASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_REALISASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})") 

                gridOptions = gd.build()
                AgGrid(tabel_cnt_mp_nilai, gridOptions=gridOptions, enable_enterprise_modules=True)    

            with grafik_cnt_4_2:

                figcntmpn = px.pie(tabel_cnt_mp_nilai, values="NILAI_REALISASI", names="METODE_PEMILIHAN", title="Grafik Pencatatan Non Tender - Nilai Transaksi - Metode Pemilihan", hole=.3)
                st.plotly_chart(figcntmpn, theme="streamlit", use_container_width=True)

        st.divider()
        
        SPSE_CNT_radio_1, SPSE_CNT_radio_2 = st.columns((2,8))
        with SPSE_CNT_radio_1:
            status_nontender_cnt = st.radio("**Status NonTender :**", df_CatatNonTender_OK_filter['status_nontender_pct_ket'].unique())
        with SPSE_CNT_radio_2:
            status_opd_cnt = st.selectbox("**Pilih Satker :**", df_CatatNonTender_OK_filter['nama_satker'].unique())

        st.divider()

        sql_CatatNonTender_query = f"""
            SELECT nama_paket AS NAMA_PAKET, jenis_realisasi AS JENIS_REALISASI, no_realisasi AS NO_REALISASI, tgl_realisasi AS TGL_REALISASI, pagu AS PAGU,
            total_realisasi AS TOTAL_REALISASI, nilai_realisasi AS NILAI_REALISASI FROM df_CatatNonTender_OK_filter
            WHERE status_nontender_pct_ket = '{status_nontender_cnt}' AND
            nama_satker = '{status_opd_cnt}'
        """

        sql_CatatNonTender_query_grafik = f"""
            SELECT kategori_pengadaan AS KATEGORI_PENGADAAN, mtd_pemilihan AS METODE_PEMILIHAN, nilai_realisasi AS NILAI_REALISASI
            FROM df_CatatNonTender_OK_filter
            WHERE status_nontender_pct_ket = '{status_nontender_cnt}' AND
            nama_satker = '{status_opd_cnt}'
        """

        df_CatatNonTender_tabel = con.execute(sql_CatatNonTender_query).df()
        df_CatatNonTender_grafik = con.execute(sql_CatatNonTender_query_grafik).df()

        data_cnt_pd_1, data_cnt_pd_2, data_cnt_pd_3, data_cnt_pd_4 = st.columns((2,3,3,2))
        data_cnt_pd_1.subheader("")
        data_cnt_pd_2.metric(label=f"Jumlah Pencatatan Non Tender ({status_nontender_cnt})", value="{:,}".format(df_CatatNonTender_tabel.shape[0]))
        data_cnt_pd_3.metric(label=f"Nilai Total Pencatatan Non Tender ({status_nontender_cnt})", value="{:,}".format(df_CatatNonTender_tabel['NILAI_REALISASI'].sum()))
        data_cnt_pd_4.subheader("")
        style_metric_cards()

        st.divider()

        gd = GridOptionsBuilder.from_dataframe(df_CatatNonTender_tabel)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("PAGU", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.PAGU.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("TOTAL_REALISASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.TOTAL_REALISASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("NILAI_REALISASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_REALISASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        
        gridOptions = gd.build()
        AgGrid(df_CatatNonTender_tabel, gridOptions=gridOptions, enable_enterprise_modules=True)
        
    #### Tab menu SPSE - Pencatatan - Swakelola
    with menu_spse_3_2:

        #### Buat tombol unduh dataset SPSE-Pencatatan-Swakelola
        unduh_CATAT_Swakelola = unduh_data(df_CatatSwakelola_OK)

        SPSE_CATAT_Swakelola_1, SPSE_CATAT_Swakelola_2 = st.columns((7,3))
        with SPSE_CATAT_Swakelola_1:
            st.subheader("Pencatatan Swakelola")
        with SPSE_CATAT_Swakelola_2:
            st.download_button(
                label = "📥 Download Data Pencatatan Swakelola",
                data = unduh_CATAT_Swakelola,
                file_name = f"SPSEPencatatanSwakelola-{kodeFolder}-{tahun}.csv",
                mime = "text/csv"
            )

        st.divider()

        sumber_dana_cs = st.radio("**Sumber Dana :**", df_CatatSwakelola_OK['sumber_dana'].unique(), key="CatatSwakelola")
        st.write(f"Anda memilih : **{sumber_dana_cs}**")

        #### Hitung-hitungan dataset Catat Swakelola
        df_CatatSwakelola_OK_filter = con.execute(f"SELECT * FROM df_CatatSwakelola_OK WHERE sumber_dana = '{sumber_dana_cs}'").df()
        jumlah_CatatSwakelola_Berjalan = con.execute(f"SELECT * FROM df_CatatSwakelola_OK_filter WHERE status_swakelola_pct_ket = 'Paket Sedang Berjalan'").df()
        jumlah_CatatSwakelola_Selesai = con.execute(f"SELECT * FROM df_CatatSwakelola_OK_filter WHERE status_swakelola_pct_ket = 'Paket Selesai'").df()
        jumlah_CatatSwakelola_Dibatalkan = con.execute(f"SELECT * FROM df_CatatSwakelola_OK_filter WHERE status_swakelola_pct_ket = 'Paket Dibatalkan'").df()

        data_cs_1, data_cs_2, data_cs_3 = st.columns(3)
        data_cs_1.metric(label="Jumlah Pencatatan Swakelola Berjalan", value="{:,}".format(jumlah_CatatSwakelola_Berjalan.shape[0]))
        data_cs_2.metric(label="Jumlah Pencacatan Swakelola Selesai", value="{:,}".format(jumlah_CatatSwakelola_Selesai.shape[0]))
        data_cs_3.metric(label="Jumlah Pencatatan Swakelola Dibatalkan", value="{:,}".format(jumlah_CatatSwakelola_Dibatalkan.shape[0]))
        style_metric_cards()

        st.divider()

        SPSE_CS_radio_1, SPSE_CS_radio_2 = st.columns((2,8))
        with SPSE_CS_radio_1:
            status_swakelola_cs = st.radio("**Status Swakelola :**", df_CatatSwakelola_OK_filter['status_swakelola_pct_ket'].unique())
        with SPSE_CS_radio_2:
            status_opd_cs = st.selectbox("**Pilih Satker :**", df_CatatSwakelola_OK_filter['nama_satker'].unique())

        st.divider()

        df_CatatSwakelola_tabel = con.execute(f"SELECT nama_paket AS NAMA_PAKET, jenis_realisasi AS JENIS_REALISASI, no_realisasi AS NO_REALISASI, tgl_realisasi AS TGL_REALISASI, pagu AS PAGU, total_realisasi AS TOTAL_REALISASI, nilai_realisasi AS NILAI_REALISASI, nama_ppk AS NAMA_PPK FROM df_CatatSwakelola_OK_filter WHERE nama_satker = '{status_opd_cs}' AND status_swakelola_pct_ket = '{status_swakelola_cs}'").df()

        data_cs_pd_1, data_cs_pd_2, data_cs_pd_3, data_cs_pd_4 = st.columns((2,3,3,2))
        data_cs_pd_1.subheader("")
        data_cs_pd_2.metric(label=f"Jumlah Pencatatan Swakelola ({status_swakelola_cs})", value="{:,}".format(df_CatatSwakelola_tabel.shape[0]))
        data_cs_pd_3.metric(label=f"Nilai Total Pencatatan Swakelola ({status_swakelola_cs})", value="{:,.2f}".format(df_CatatSwakelola_tabel['NILAI_REALISASI'].sum()))
        data_cs_pd_4.subheader("")
        style_metric_cards()

        gd = GridOptionsBuilder.from_dataframe(df_CatatSwakelola_tabel)
        gd.configure_pagination()
        gd.configure_side_bar()
        gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gd.configure_column("PAGU", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.PAGU.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("TOTAL_REALISASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.TOTAL_REALISASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        gd.configure_column("NILAI_REALISASI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_REALISASI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
        
        gridOptions = gd.build()
        AgGrid(df_CatatSwakelola_tabel, gridOptions=gridOptions, enable_enterprise_modules=True)

## Tab menu SPSE - Peserta Tender
with menu_spse_4:

    df_RUPMasterSatker_filter_pt = df_RUPMasterSatker[["kd_satker_str", "nama_satker"]]
    df_SPSETenderPengumuman_filter_pt = df_SPSETenderPengumuman[["kd_tender", "nama_paket", "pagu", "hps", "sumber_dana"]]
    #df_SPSETenderKontrak_filter_pt_OK = con.execute(f"SELECT DISTINCT(kd_penyedia), wakil_sah_penyedia FROM df_SPSETenderKontrak").df()

    df_PesertaTenderDetail_1 = df_PesertaTender.merge(df_RUPMasterSatker_filter_pt, how='left', on='kd_satker_str')
    df_PesertaTenderDetail_2 = df_PesertaTenderDetail_1.merge(df_SPSETenderPengumuman_filter_pt, how='left', on='kd_tender')
    #df_PesertaTenderDetail_3 = df_PesertaTenderDetail_2.merge(df_SPSETenderKontrak_filter_pt_OK, how='left', on='kd_penyedia')

    #### Buat tombol unduh dataset Peserta Tender
    unduh_Peserta_Tender = unduh_data(df_PesertaTenderDetail_2)

    SPSE_PT_D_1, SPSE_PT_D_2 = st.columns((7,3))
    with SPSE_PT_D_1:
        st.header(f"SPSE - Peserta Tender - {pilih}")
    with SPSE_PT_D_2:
        st.download_button(
            label = "📥 Download Data Peserta Tender",
            data = unduh_Peserta_Tender,
            file_name = f"SPSEPesertaTenderDetail-{kodeFolder}-{tahun}.csv",
            mime = "text/csv"
        )

    st.divider()

    sumber_dana_pt = st.radio("**Sumber Dana :**", df_PesertaTenderDetail_2['sumber_dana'].unique(), key="PesertaTender")
    st.write(f"Anda memilih : **{sumber_dana_pt}**")

    #### Hitung-hitungan dataset Peserta Tender
    df_PesertaTenderDetail_filter = df_PesertaTenderDetail_2.query(f"sumber_dana == '{sumber_dana_pt}'")
    jumlah_PesertaTender_daftar = df_PesertaTenderDetail_filter.query("nilai_penawaran == 0 and nilai_terkoreksi == 0")
    jumlah_PesertaTender_nawar = df_PesertaTenderDetail_filter.query("nilai_penawaran > 0 and nilai_terkoreksi > 0")
    jumlah_PesertaTender_menang = df_PesertaTenderDetail_filter.query("nilai_penawaran > 0 and nilai_terkoreksi > 0 and pemenang == 1")

    data_pt_1, data_pt_2, data_pt_3, data_pt_4 = st.columns(4)
    data_pt_1.metric(label="Jumlah Peserta Yang Mendaftar", value="{:,}".format(jumlah_PesertaTender_daftar.shape[0]))
    data_pt_2.metric(label="Jumlah Peserta Yang Menawar", value="{:,}".format(jumlah_PesertaTender_nawar.shape[0]))
    data_pt_3.metric(label="Jumlah Peserta Yang Menang", value="{:,}".format(jumlah_PesertaTender_menang.shape[0]))
    data_pt_4.metric(label="Nilai Total Terkoreksi (Pemenang)", value="{:,.2f}".format(jumlah_PesertaTender_menang['nilai_terkoreksi'].sum()))
    style_metric_cards()

    st.divider()

    SPSE_PT_radio_1, SPSE_PT_radio_2 = st.columns((2,8))
    with SPSE_PT_radio_1:
        status_pemenang_pt = st.radio("**Tabel Data Peserta :**", ["PEMENANG", "MENDAFTAR", "MENAWAR"])
    with SPSE_PT_radio_2:
        status_opd_pt = st.selectbox("**Pilih Satker :**", df_PesertaTenderDetail_filter['nama_satker'].unique())

    st.divider()

    if status_pemenang_pt == "PEMENANG":
        jumlah_PeserteTender = con.execute(f"SELECT nama_paket AS NAMA_PAKET, nama_penyedia AS NAMA_PENYEDIA, npwp_penyedia AS NPWP_PENYEDIA, pagu AS PAGU, hps AS HPS, nilai_penawaran AS NILAI_PENAWARAN, nilai_terkoreksi AS NILAI_TERKOREKSI FROM df_PesertaTenderDetail_filter WHERE NAMA_SATKER = '{status_opd_pt}' AND NILAI_PENAWARAN > 0 AND NILAI_TERKOREKSI > 0  AND pemenang = 1").df()
    elif status_pemenang_pt == "MENDAFTAR":
        jumlah_PeserteTender = con.execute(f"SELECT nama_paket AS NAMA_PAKET, nama_penyedia AS NAMA_PENYEDIA, npwp_penyedia AS NPWP_PENYEDIA, pagu AS PAGU, hps AS HPS, nilai_penawaran AS NILAI_PENAWARAN, nilai_terkoreksi AS NILAI_TERKOREKSI FROM df_PesertaTenderDetail_filter WHERE NAMA_SATKER = '{status_opd_pt}' AND NILAI_PENAWARAN = 0 AND NILAI_TERKOREKSI = 0").df()
    else:
        jumlah_PeserteTender = con.execute(f"SELECT nama_paket AS NAMA_PAKET, nama_penyedia AS NAMA_PENYEDIA, npwp_penyedia AS NPWP_PENYEDIA, pagu AS PAGU, hps AS HPS, nilai_penawaran AS NILAI_PENAWARAN, nilai_terkoreksi AS NILAI_TERKOREKSI FROM df_PesertaTenderDetail_filter WHERE NAMA_SATKER = '{status_opd_pt}' AND NILAI_PENAWARAN > 0 AND NILAI_TERKOREKSI > 0").df()

    data_pt_pd_1, data_pt_pd_2, data_pt_pd_3, data_pt_pd_4 = st.columns(4)
    data_pt_pd_1.subheader("")
    data_pt_pd_2.metric(label=f"Jumlah Peserta Tender ({status_pemenang_pt})", value="{:,}".format(jumlah_PeserteTender.shape[0]))
    data_pt_pd_3.metric(label=f"Nilai Total Terkoreksi ({status_pemenang_pt})", value="{:,.2f}".format(jumlah_PeserteTender['NILAI_TERKOREKSI'].sum()))
    data_pt_pd_4.subheader("")
    style_metric_cards()

    gd = GridOptionsBuilder.from_dataframe(jumlah_PeserteTender)
    gd.configure_pagination()
    gd.configure_side_bar()
    gd.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    gd.configure_column("PAGU", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.PAGU.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("HPS", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.HPS.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("NILAI_PENAWARAN", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_PENAWARAN.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    gd.configure_column("NILAI_TERKOREKSI", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueGetter = "data.NILAI_TERKOREKSI.toLocaleString('id-ID', {style: 'currency', currency: 'IDR', maximumFractionDigits:2})")
    
    gridOptions = gd.build()
    AgGrid(jumlah_PeserteTender, gridOptions=gridOptions, enable_enterprise_modules=True)