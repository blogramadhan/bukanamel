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

import streamlit as st

st.set_page_config(
    page_title="Dashboard Pengadaan Barang dan Jasa",
    page_icon="👋",
    layout="wide"
)

st.title("Dashboard Pengadaan Barang dan Jasa")

st.markdown("""
*Dashboard* ini dibuat sebagai alat bantu untuk mempermudah para pelaku pengadaan di seluruh wilayah Provinsi Kalimantan Barat. Data yang disajikan, antara lain:
* **Perencanaan**
  * Profil RUP Daerah
  * Profil RUP Perangkat Daerah
  * Struktur Anggaran
  * % Input RUP
  * RUP Paket Penyedia Perangkat Daerah
  * RUP Paket Swakelola Perangkat Daerah
* **Persiapan**
* **Pemilihan**
* **Kontrak**
* **Serah Terima**
* **Monitoring**

Dibuat dengan menggunakan bahasa [Python](https://www.python.org/) dengan tambahan beberapa *library* utama seperti:
* [Pandas](https://pandas.pydata.org/)
* [Streamlit](https://streamlit.io)
* [DuckDB](https://duckdb.org)

Sumber data dari *Dashboard* ini berasal dari **API JSON Versi 2** yang ditarik harian dari [ISB LKPP](https://lkpp.go.id). 

@2023 - **UlarKadut** 
""")