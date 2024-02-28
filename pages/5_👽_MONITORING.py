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
import numpy as np
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
con = duckdb.connect(database=':memory:')
# duckdb.sql("INSTALL httpfs")
# duckdb.sql("LOAD httpfs")

## Akses file dataset format parquet dari Google Cloud Storage via URL public

### Dataset SIRUP
DatasetRUPPP = f"https://data.pbj.my.id/{kodeRUP}/sirup/RUP-PaketPenyedia-Terumumkan{tahun}.parquet"
DatasetRUPPS = f"https://data.pbj.my.id/{kodeRUP}/sirup/RUP-PaketSwakelola-Terumumkan{tahun}.parquet"
DatasetRUPSA = f"https://data.pbj.my.id/{kodeRUP}/sirup/RUP-StrukturAnggaranPD{tahun}.parquet"

### Dataset Tender
DatasetSPSETenderPengumuman = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-TenderPengumuman{tahun}.parquet"
DatasetSPSETenderKontrak = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-TenderEkontrak-Kontrak{tahun}.parquet"

### Dataset Non Tender
DatasetSPSENonTenderPengumuman = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-NonTenderPengumuman{tahun}.parquet"

### Dataset E-Purchasing
DatasetPURCHASINGECAT = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Ecat-PaketEPurchasing{tahun}.parquet"

### Dataset Toko Daring
DatasetPURCHASINGBELA = f"https://data.pbj.my.id/{kodeRUP}/epurchasing/Bela-TokoDaringRealisasi{tahun}.parquet"

### Dataset SPSE Tender dan SIKAP
DatasetSPSETenderPengumuman = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-TenderPengumuman{tahun}.parquet"
DatasetSIKAPTender = f"https://data.pbj.my.id/{kodeRUP}/sikap/SIKaP-PenilaianKinerjaPenyedia-Tender{tahun}.parquet"

### Dataset SPSE Non Tender dan SIKAP
DatasetSPSENonTenderPengumuman = f"https://data.pbj.my.id/{kodeLPSE}/spse/SPSE-NonTenderPengumuman{tahun}.parquet"
DatasetSIKAPNonTender = f"https://data.pbj.my.id/{kodeRUP}/sikap/SiKAP-PenilaianKinerjaPenyedia-NonTender{tahun}.parquet"

#####
# Mulai membuat presentasi data Purchasing
#####

# Buat menu yang mau disajikan
menu_monitoring_1, menu_monitoring_2 = st.tabs(["| ITKP |", "| SIKAP |"])

## Tab menu monitoring ITKP
with menu_monitoring_1:

    st.header(f"PREDIKSI ITKP PEMANFAATAN SISTEM PENGADAAN - {pilih} - TAHUN {tahun}")

    st.divider()

    try:
        ### PREDIKSI ITKP SIRUP
        #### Tarik dataset SIRUP
        df_RUPPP = tarik_data_pd(DatasetRUPPP)
        df_RUPPS = tarik_data_pd(DatasetRUPPS)
        df_RUPSA = tarik_data_pd(DatasetRUPSA)

        #### Query RUP Paket Penyedia
        df_RUPPP_umumkan = con.execute("SELECT * FROM df_RUPPP WHERE status_umumkan_rup = 'Terumumkan' AND status_aktif_rup = 'TRUE' AND metode_pengadaan <> '0'").df()
        RUPPS_umumkan_sql = """
            SELECT nama_satker, kd_rup, nama_paket, pagu, tipe_swakelola, volume_pekerjaan, uraian_pekerjaan, 
            tgl_pengumuman_paket, tgl_awal_pelaksanaan_kontrak, nama_ppk, status_umumkan_rup
            FROM df_RUPPS
            WHERE status_umumkan_rup = 'Terumumkan'
        """
        df_RUPPS_umumkan = con.execute(RUPPS_umumkan_sql).df()

        belanja_pengadaan = df_RUPSA['belanja_pengadaan'].sum()
        nilai_total_rup = df_RUPPP_umumkan['pagu'].sum() + df_RUPPS_umumkan['pagu'].sum()
        persen_capaian_rup = nilai_total_rup / belanja_pengadaan
        if persen_capaian_rup > 1:
            prediksi_itkp_rup = (1 - (persen_capaian_rup - 1)) * 10
        elif persen_capaian_rup > 0.5:
            prediksi_itkp_rup = persen_capaian_rup * 10 
        else:
            prediksi_itkp_rup = 0
        ### END ITKP SIRUP

        ### Tampilan Prediksi ITKP
        st.subheader("**RENCANA UMUM PENGADAAN**")
        itkp_sirup_1, itkp_sirup_2, itkp_sirup_3, itkp_sirup_4 = st.columns(4)
        itkp_sirup_1.metric(label="BELANJA PENGADAAN (MILYAR)", value="{:,.2f}".format(belanja_pengadaan / 1000000000))
        itkp_sirup_2.metric(label="NILAI RUP (MILYAR)", value="{:,.2f}".format(nilai_total_rup / 1000000000))
        itkp_sirup_3.metric(label="PERSENTASE", value="{:.2%}".format(persen_capaian_rup))
        itkp_sirup_4.metric(label="NILAI PREDIKSI (DARI 10)", value="{:,}".format(round(prediksi_itkp_rup, 2)))
        style_metric_cards()

    except Exception:
        st.error("GAGAL ANALISA PREDIKSI ITKP SIRUP")

        ###

    try:
        ### PREDIKSI ITKP E-TENDERING
        #### Tarik dataset SIRUP + SPSE E-TENDERING
        df_SPSETenderPengumuman = tarik_data_pd(DatasetSPSETenderPengumuman)
        df_SPSETenderPengumuman_etendering = con.execute("SELECT kd_tender, pagu, hps FROM df_SPSETenderPengumuman WHERE status_tender = 'Selesai'").df()
        df_RUPPP_umumkan_etendering = con.execute("SELECT pagu FROM df_RUPPP_umumkan WHERE metode_pengadaan IN ('Tender', 'Tender Cepat')").df()

        #### Query ITKP E-TENDERING
        nilai_etendering_rup = df_RUPPP_umumkan_etendering['pagu'].sum()
        nilai_etendering_spse = df_SPSETenderPengumuman_etendering['pagu'].sum()
        persen_capaian_etendering = nilai_etendering_spse / nilai_etendering_rup        
        if persen_capaian_etendering > 1:
            prediksi_itkp_etendering = (1 - (persen_capaian_etendering - 1)) * 5
        elif persen_capaian_etendering > 0.5:
            prediksi_itkp_etendering = persen_capaian_etendering * 5 
        else:
            prediksi_itkp_etendering = 0
        #### END ITKP ETENDERING
            
        ### Tampilan Prediksi E-TENDERING
        st.subheader("**E-TENDERING**")
        itkp_etendering_1, itkp_etendering_2, itkp_etendering_3, itkp_etendering_4 = st.columns(4)
        itkp_etendering_1.metric(label="NILAI RUP E-TENDERING (MILYAR)", value="{:,.2f}".format(nilai_etendering_rup / 1000000000))
        itkp_etendering_2.metric(label="E-TENDERING SELESAI (MILYAR)", value="{:,.2f}".format(nilai_etendering_spse / 1000000000))
        itkp_etendering_3.metric(label="PERSENTASE", value="{:.2%}".format(persen_capaian_etendering))
        itkp_etendering_4.metric(label="NILAI PREDIKSI (DARI 5)", value="{:,}".format(round(prediksi_itkp_etendering, 2)))
        style_metric_cards()

    except Exception:
        st.error("GAGAL ANALISA PREDIKSI ITKP E-TENDERING")

        ###

    try:
        ### PREDIKSI ITKP NON E-TENDERING
        #### Tarik dataset SIRUP + SPSE NON E-TENDERING
        df_SPSENonTenderPengumuman = tarik_data_pd(DatasetSPSENonTenderPengumuman)
        df_SPSENonTenderPengumuman_filter = con.execute("SELECT pagu, hps FROM df_SPSENonTenderPengumuman WHERE status_nontender = 'Selesai'").df()
        df_RUPPP_umumkan_non_etendering = con.execute("SELECT pagu FROM df_RUPPP_umumkan WHERE metode_pengadaan IN ('Pengadaan Langsung', 'Penunjukan Langsung')").df()

        #### Query ITKP NON E-TENDERING
        nilai_nonetendering_rup = df_RUPPP_umumkan_non_etendering['pagu'].sum()
        nilai_nonetendering_spse = df_SPSENonTenderPengumuman_filter['pagu'].sum()
        persen_capaian_nonetendering = nilai_nonetendering_spse / nilai_nonetendering_rup        
        if persen_capaian_nonetendering > 1:
            prediksi_itkp_nonetendering = (1 - (persen_capaian_nonetendering - 1)) * 5
        elif persen_capaian_nonetendering > 0.5:
            prediksi_itkp_nonetendering = persen_capaian_nonetendering * 5 
        else:
            prediksi_itkp_nonetendering = 0
        #### END ITKP NON E-TENDERING
            
        ### Tampilan Prediksi NONE-TENDERING
        st.subheader("**NON E-TENDERING**")
        itkp_nonetendering_1, itkp_nonetendering_2, itkp_nonetendering_3, itkp_nonetendering_4 = st.columns(4)
        itkp_nonetendering_1.metric(label="NILAI RUP NON E-TENDERING (MILYAR)", value="{:,.2f}".format(nilai_nonetendering_rup / 1000000000))
        itkp_nonetendering_2.metric(label="NON E-TENDERING SELESAI (MILYAR)", value="{:,.2f}".format(nilai_nonetendering_spse / 1000000000))
        itkp_nonetendering_3.metric(label="PERSENTASE", value="{:.2%}".format(persen_capaian_nonetendering))
        itkp_nonetendering_4.metric(label="NILAI PREDIKSI (DARI 5)", value="{:,}".format(round(prediksi_itkp_nonetendering, 2)))
        style_metric_cards()

    except Exception:
        st.error("GAGAL ANALISA PREDIKSI ITKP NON E-TENDERING")

        ###

    try:
        ### PREDIKSI ITKP E-KONTRAK
        #### Tarik dataset E-KONTRAK
        df_SPSETenderKontrak = tarik_data_pd(DatasetSPSETenderKontrak)
        df_SPSETenderKontrak_filter = con.execute("SELECT * kd_tender FROM df_SPSETenderKontrak").df()
        
        #### Query ITKP E-KONTRAK
        jumlah_tender_selesai = df_SPSETenderPengumuman_etendering['kd_tender'].count()
        jumlah_tender_kontrak = df_SPSETenderKontrak_filter['kd_tender'].count()
        persen_capaian_ekontrak = jumlah_tender_kontrak / jumlah_tender_selesai        
        if persen_capaian_ekontrak > 1:
            prediksi_itkp_ekontrak = (1 - (persen_capaian_ekontrak - 1)) * 5
        elif persen_capaian_ekontrak > 0.2:
            prediksi_itkp_ekontrak = persen_capaian_ekontrak * 5 
        else:
            prediksi_itkp_ekontrak = 0
        #### END ITKP E-KONTRAK
            
        ### Tampilan Prediksi E-KONTRAK
        st.subheader("**E-KONTRAK**")
        itkp_ekontrak_1, itkp_ekontrak_2, itkp_ekontrak_3, itkp_ekontrak_4 = st.columns(4)
        itkp_ekontrak_1.metric(label="JUMLAH PAKET TENDER SELESAI", value="{:,}".format(jumlah_tender_selesai))
        itkp_ekontrak_2.metric(label="JUMLAH PAKET TENDER BERKONTRAK", value="{:,}".format(jumlah_tender_kontrak))
        itkp_ekontrak_3.metric(label="PERSENTASE", value="{:.2%}".format(persen_capaian_ekontrak))
        itkp_ekontrak_4.metric(label="NILAI PREDIKSI (DARI 5)", value="{:,}".format(round(prediksi_itkp_ekontrak, 2)))
        style_metric_cards()

    except Exception:
        st.error("GAGAL ANALISA PREDIKSI ITKP E-KONTRAK")

        ###

    try:
        ### PREDIKSI ITKP E-PURCHASING
        #### Tarik dataset SIRUP + SPSE E-PURCHASING
        df_ECAT = tarik_data_pd(DatasetPURCHASINGECAT)
        df_ECAT_filter = con.execute("SELECT total_harga FROM df_ECAT WHERE paket_status_str IN ('Paket Selesai', 'Paket Proses')").df()
        df_RUPPP_umumkan_epurchasing = con.execute("SELECT pagu FROM df_RUPPP_umumkan WHERE metode_pengadaan = 'e-Purchasing'").df()

        #### Query ITKP E-PURCHASING
        nilai_epurchasing_rup = df_RUPPP_umumkan_epurchasing['pagu'].sum()
        nilai_epurchasing_ecat = df_ECAT_filter['total_harga'].sum()
        persen_capaian_epurchasing = nilai_epurchasing_ecat / nilai_epurchasing_rup        
        if persen_capaian_epurchasing > 1:
            prediksi_itkp_epurchasing = (1 - (persen_capaian_epurchasing - 1)) * 4
        elif persen_capaian_epurchasing > 0.5:
            prediksi_itkp_epurchasing = persen_capaian_epurchasing * 4 
        else:
            prediksi_itkp_epurchasing = 0
        #### END ITKP E-PURCHASING
            
        ### Tampilan Prediksi E-PURCHASING
        st.subheader("**E-PURCHASING**")
        itkp_epurchasing_1, itkp_epurchasing_2, itkp_epurchasing_3, itkp_epurchasing_4 = st.columns(4)
        itkp_epurchasing_1.metric(label="NILAI RUP E-PURCHASING (MILYAR)", value="{:,.2f}".format(nilai_epurchasing_rup / 1000000000))
        itkp_epurchasing_2.metric(label="E-PURCHASING SELESAI (MILYAR)", value="{:,.2f}".format(nilai_epurchasing_ecat / 1000000000))
        itkp_epurchasing_3.metric(label="PERSENTASE", value="{:.2%}".format(persen_capaian_epurchasing))
        itkp_epurchasing_4.metric(label="NILAI PREDIKSI (DARI 4)", value="{:,}".format(round(prediksi_itkp_epurchasing, 2)))
        style_metric_cards()

    except Exception:
        st.error("GAGAL ANALISA PREDIKSI ITKP E-PURCHASING")

        ###

    try:
        ### PREDIKSI ITKP TOKO DARING
        #### Tarik dataset TOKO DARING
        df_BELA = tarik_data_pd(DatasetPURCHASINGBELA)
        df_BELA_filter = con.execute(f"SELECT valuasi FROM df_BELA WHERE nama_satker IS NOT NULL AND status_verif = 'verified' AND status_konfirmasi_ppmse = 'selesai'").df()
        
        #### Query ITKP TOKO DARING
        jumlah_trx_bela = df_BELA_filter['valuasi'].count()
        nilai_trx_bela = df_BELA_filter['valuasi'].sum()
        if jumlah_trx_bela >= 1:
            prediksi_itkp_bela = 1
        else:
            prediksi_itkp_bela = 0
        #### END ITKP TOKO DARING
            
        ### Tampilan Prediksi TOKO DARING
        st.subheader("**TOKO DARING**")
        itkp_bela_1, itkp_bela_2, itkp_bela_3 = st.columns(3)
        itkp_bela_1.metric(label="JUMLAH TRANSAKSI TOKO DARING", value="{:,}".format(jumlah_trx_bela))
        itkp_bela_2.metric(label="NILAI TRANSAKSI TOKO DARING", value="{:,.2f}".format(nilai_trx_bela))
        itkp_bela_3.metric(label="NILAI PREDIKSI (DARI 1)", value="{:,}".format(round(prediksi_itkp_bela, 2)))
        style_metric_cards()

    except Exception:
            st.error("GAGAL ANALISA PREDIKSI ITKP TOKO DARING")

with menu_monitoring_2:

    st.header(f"MONITORING SIKAP - {pilih} - TAHUN {tahun}")

    ### Buat sub menu SIKAP
    menu_monitoring_2_1, menu_monitoring_2_2 = st.tabs(["| SIKAP TENDER |", "| SIKAP NON TENDER |"])

    #### Tab menu SIKAP - TENDER
    with menu_monitoring_2_1:

        try:
            ##### Tarik dataset SIKAP TENDER
            df_SPSETenderPengumuman = tarik_data_pd(DatasetSPSETenderPengumuman)
            df_SIKAPTender = tarik_data_pd(DatasetSIKAPTender)

            ##### Buat tombol undah dataset SIKAP TENDER

            st.subheader("SIKAP TENDER")

            st.divider()

            df_SPSETenderPengumuman_filter = con.execute(f"SELECT kd_tender, nama_satker, pagu, hps, jenis_pengadaan, mtd_pemilihan, FROM df_SPSETenderPengumuman WHERE status_tender = 'Selesai'").df()
            df_SIKAPTender_filter = con.execute(f"SELECT kd_tender, nama_paket, nama_ppk, nama_penyedia, npwp_penyedia, indikator_penilaian, nilai_indikator, total_skors FROM df_SIKAPTender").df()
            df_SIKAPTender_OK = df_SPSETenderPengumuman_filter.merge(df_SIKAPTender_filter, how='right', on='kd_tender')

            jumlah_trx_spse_t_pengumuman = df_SPSETenderPengumuman_filter['kd_tender'].unique().shape[0]
            jumlah_trx_sikap_t = df_SIKAPTender_filter['kd_tender'].unique().shape[0]
            selisih_sikap_t = jumlah_trx_spse_t_pengumuman - jumlah_trx_sikap_t

            data_sikap_t_1, data_sikap_t_2, data_sikap_t_3 = st.columns(3)
            data_sikap_t_1.metric(label="Jumlah Paket Tender Selesai", value="{:,}".format(jumlah_trx_spse_t_pengumuman))
            data_sikap_t_2.metric(label="Jumlah Paket Tender Sudah Dinilai", value="{:,}".format(jumlah_trx_sikap_t))
            data_sikap_t_3.metric(label="Jumlah Paket Tender Belum Dinilai", value="{:,}".format(selisih_sikap_t))
            style_metric_cards()

            st.divider()

            df_SIKAPTender_OK_filter = con.execute("SELECT nama_paket AS NAMA_PAKET, kd_tender AS KODE_PAKET, jenis_pengadaan AS JENIS_PENGADAAN, nama_ppk AS NAMA_PPK, nama_penyedia AS NAMA_PENYEDIA, AVG(total_skors) AS SKOR_PENILAIAN FROM df_SIKAPTender_OK GROUP BY KODE_PAKET, NAMA_PAKET, JENIS_PENGADAAN, NAMA_PPK, NAMA_PENYEDIA").df()
            df_SIKAPTender_OK_filter_final = df_SIKAPTender_OK_filter.assign(KETERANGAN = np.where(df_SIKAPTender_OK_filter['SKOR_PENILAIAN'] >= 3, "SANGAT BAIK", np.where(df_SIKAPTender_OK_filter['SKOR_PENILAIAN'] >= 2, "BAIK", np.where(df_SIKAPTender_OK_filter['SKOR_PENILAIAN'] >= 1, "CUKUP", "BURUK"))))

            unduh_SIKAP_Tender_excel = download_excel(df_SIKAPTender_OK_filter_final)

            st.download_button(
                label = "📥 Download Data SIKAP Tender",
                data = unduh_SIKAP_Tender_excel,
                file_name = f"SIKAPTender-{kodeFolder}-{tahun}.xlsx",
                mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )        

            gd_sikap_t = GridOptionsBuilder.from_dataframe(df_SIKAPTender_OK_filter_final)
            gd_sikap_t.configure_pagination()
            gd_sikap_t.configure_side_bar()
            gd_sikap_t.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            
            AgGrid(df_SIKAPTender_OK_filter_final, gridOptions=gd_sikap_t.build(), enable_enterprise_modules=True)

        except Exception:
            st.error("Gagal baca dataset SIKAP TENDER")

    with menu_monitoring_2_2:

        try:
            ##### Tarik dataset SIKAP NON TENDER
            df_SPSENonTenderPengumuman = tarik_data_pd(DatasetSPSENonTenderPengumuman)
            df_SIKAPNonTender = tarik_data_pd(DatasetSIKAPNonTender)

            ##### Buat tombol undah dataset SIKAP NON TENDER

            st.subheader("SIKAP NON TENDER")

            st.divider()

            df_SPSENonTenderPengumuman_filter = con.execute(f"SELECT kd_nontender, nama_satker, pagu, hps, jenis_pengadaan, mtd_pemilihan, FROM df_SPSENonTenderPengumuman WHERE status_nontender = 'Selesai'").df()
            df_SIKAPNonTender_filter = con.execute(f"SELECT kd_nontender, nama_paket, nama_ppk, nama_penyedia, npwp_penyedia, indikator_penilaian, nilai_indikator, total_skors FROM df_SIKAPNonTender").df()
            df_SIKAPNonTender_OK = df_SPSENonTenderPengumuman_filter.merge(df_SIKAPNonTender_filter, how='right', on='kd_nontender')

            jumlah_trx_spse_nt_pengumuman = df_SPSENonTenderPengumuman_filter['kd_nontender'].unique().shape[0]
            jumlah_trx_sikap_nt = df_SIKAPNonTender_filter['kd_nontender'].unique().shape[0]
            selisih_sikap_nt = jumlah_trx_spse_nt_pengumuman - jumlah_trx_sikap_nt

            data_sikap_nt_1, data_sikap_nt_2, data_sikap_nt_3 = st.columns(3)
            data_sikap_nt_1.metric(label="Jumlah Paket Non Tender", value="{:,}".format(jumlah_trx_spse_nt_pengumuman))
            data_sikap_nt_2.metric(label="Jumlah Paket Sudah Dinilai", value="{:,}".format(jumlah_trx_sikap_nt))
            data_sikap_nt_3.metric(label="Jumlah Paket Belum Dinilai", value="{:,}".format(selisih_sikap_nt))
            style_metric_cards()

            st.divider()

            df_SIKAPNonTender_OK_filter = con.execute("SELECT nama_paket AS NAMA_PAKET, kd_nontender AS KODE_PAKET, jenis_pengadaan AS JENIS_PENGADAAN, nama_ppk AS NAMA_PPK, nama_penyedia AS NAMA_PENYEDIA, AVG(total_skors) AS SKOR_PENILAIAN FROM df_SIKAPNonTender_OK GROUP BY KODE_PAKET, NAMA_PAKET, JENIS_PENGADAAN, NAMA_PPK, NAMA_PENYEDIA").df()
            df_SIKAPNonTender_OK_filter_final = df_SIKAPNonTender_OK_filter.assign(KETERANGAN = np.where(df_SIKAPNonTender_OK_filter['SKOR_PENILAIAN'] >= 3, "SANGAT BAIK", np.where(df_SIKAPNonTender_OK_filter['SKOR_PENILAIAN'] >= 2, "BAIK", np.where(df_SIKAPNonTender_OK_filter['SKOR_PENILAIAN'] >= 1, "CUKUP", "BURUK"))))

            unduh_SIKAP_NonTender_excel = download_excel(df_SIKAPNonTender_OK_filter_final)

            st.download_button(
                label = "📥 Download Data SIKAP Non Tender",
                data = unduh_SIKAP_NonTender_excel,
                file_name = f"SIKAPNonTender-{kodeFolder}-{tahun}.xlsx",
                mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )        

            gd_sikap_nt = GridOptionsBuilder.from_dataframe(df_SIKAPNonTender_OK_filter_final)
            gd_sikap_nt.configure_pagination()
            gd_sikap_nt.configure_side_bar()
            gd_sikap_nt.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            
            AgGrid(df_SIKAPNonTender_OK_filter_final, gridOptions=gd_sikap_nt.build(), enable_enterprise_modules=True)

            # st.dataframe(
            #     df_SIKAPNonTender_OK_filter_final, 
            #     column_config = {
            #         "NAMA_PAKET": "NAMA PAKET",
            #         "KODE_PAKET": "KODE PAKET",
            #         "JENIS_PENGADAAN": "JENIS PENGADAAN",
            #         "NAMA_PPK": "PPK",
            #         "NAMA_PENYEDIA": "PENYEDIA",
            #         "SKOR_PENILAIAN": "SKOR",
            #         "KETERANGAN": "KETERANGAN"
            #     },
            #     use_container_width = True,
            #     hide_index = True,
            # )

        except Exception:
            st.error("Gagal baca dataset SIKAP NON TENDER")