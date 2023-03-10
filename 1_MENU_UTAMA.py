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
    page_title="Dashboard Bukan Amel",
    page_icon="👋",
    layout="wide"
)

st.title("Dashboard Bukan Amel")

st.markdown("""
*Dashboard* ini dibuat sebagai alat bantu untuk mempermudah para pelaku pengadaan di seluruh wilayah Provinsi Kalimantan Barat. Data yang disajikan, antara lain:
* **Perencanaan**
  * Rekap Perencanaan
  * Detail Paket
  * Rekap Satker
* **Persiapan**
  * Rekap Persiapan
  * Detail Paket
* **Pemilihan**
  * Rekap Pemilihan
  * Detail Paket
* **Kontrak**
  * Rekap Kontrak
  * Detail Paket
* **Serah Terima**
  * Rekap Serah Terima
  * Detail Paket
* **Monitoring**
  * Monitoring Paket
  * Struktur Anggaran
  * Toko Daring
  * Warning RUP

*Made with love* dengan menggunakan bahasa programming [Python](https://www.python.org/) dengan beberapa *library* utama seperti:
* [Pandas](https://pandas.pydata.org/)
* [Streamlit](https://streamlit.io)
* [DuckDB](https://duckdb.org)

Sumber data dari *Dashboard* ini berasal dari **API JSON** yang ditarik harian dari [ISB LKPP](https://lkpp.go.id). Data tersebut kemudian disimpan di [Google Cloud Storage](https://google.com) untuk kemudian diolah lebih lanjut dengan [Python](https://python.org).

@2022 - **UlarKadut** 
""")