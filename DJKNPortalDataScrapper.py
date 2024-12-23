import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

class DJKNPortalDataScraper:
    def __init__(self, url_input="https://djpk.kemenkeu.go.id/portal/data/apbd"):
        self.url_input = url_input
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36"
        }
        self.provinsi = {}
        self.kabupaten_kota = {}

    def fetch_provinsi(self):
        response = requests.get(self.url_input, headers=self.headers)
        if response.status_code == 200:
            print("Request berhasil! HTML telah diambil.")
            html_content = response.text
            soup = BeautifulSoup(html_content, "html.parser")
            select_provinsi = soup.find("select", {"id": "sel_provinsi"})
            if select_provinsi:
                options = select_provinsi.find_all("option")
                self.provinsi = {option['value']: option.text for option in options}
                print("Informasi provinsi:", self.provinsi)
            else:
                print("Elemen select tidak ditemukan.")
        else:
            raise Exception(f"Request gagal dengan status code: {response.status_code}")

    def fetch_kabupaten_kota(self, kode_provinsi, tahun_options):
        for kode in kode_provinsi:
            for tahun in tahun_options:
                url = f"https://djpk.kemenkeu.go.id/portal/pemda/{kode}/{tahun}"
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.kabupaten_kota[(kode, tahun)] = data
                        print(f"Data kabupaten/kota untuk provinsi {kode} tahun {tahun}:", data)
                    except ValueError:
                        print(f"Respon bukan JSON untuk provinsi {kode} tahun {tahun}.")
                else:
                    print(f"Gagal mengambil data untuk provinsi {kode} tahun {tahun}. Status code: {response.status_code}")

    def fetch_apbd_table(self, kode_provinsi, tahun_options, periode_options):
        for kode in tqdm(kode_provinsi, desc="Memproses Provinsi"):
            for tahun in tqdm(tahun_options, desc=f"Memproses Tahun {kode}", leave=False):
                for periode in tqdm(periode_options, desc=f"Memproses Periode {tahun}", leave=False):
                    for kode_kabupaten, nama_kabupaten in tqdm(self.kabupaten_kota.get((kode, tahun), {}).items(), desc=f"Memproses Kabupaten/Kota {periode}", leave=False):
                        payload = {
                            "periode": periode,
                            "tahun": tahun,
                            "provinsi": kode,
                            "pemda": kode_kabupaten
                        }
                        response = requests.get(self.url_input, data=payload, headers=self.headers)
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.text, "html.parser")
                            table = soup.find("table")
                            if table:
                                print("Tabel ditemukan!")
                                rows = table.find_all("tr")
                                cell_values = []
                                for row in rows:
                                    cells = row.find_all(["td", "th"])
                                    cell_values.extend([cell.get_text(strip=True) for cell in cells])

                                flattened_data = []
                                current_row = []
                                for cell in cell_values:
                                    if cell == '':
                                        if current_row:
                                            flattened_data.append(current_row)
                                            current_row = []
                                    else:
                                        current_row.append(cell)
                                if current_row:
                                    flattened_data.append(current_row)

                                if flattened_data:
                                    header = flattened_data[0]
                                    structured_data = flattened_data[1:]
                                    df = pd.DataFrame(structured_data, columns=header)

                                    if "Akun" in df.columns:
                                        df_cleaned = df.drop_duplicates(subset=['Akun'], keep='first')
                                    else:
                                        df_cleaned = df

                                    nama_provinsi = self.provinsi.get(kode, "Unknown")
                                    filename = f"apbd_{nama_provinsi}_{tahun}_{periode}_{nama_kabupaten}.csv"
                                    df_cleaned.to_csv(filename, index=False, encoding="utf-8")
                                    print(f"Data berhasil disimpan ke {filename}")
                            else:
                                print("Tabel tidak ditemukan.")
                        else:
                            print(f"Gagal mengambil data tabel. Status code: {response.status_code}")

if __name__ == "__main__":
    scraper = DJKNPortalDataScraper()
    scraper.fetch_provinsi()

    # User memilih periode, tahun, dan provinsi
    periode_options = ["1"]  # Contoh periode Januari, 2 adalah Februari, 3 adalah Maret dst.
    tahun_options = ["2015"]  # Contoh tahun 2015
    kode_provinsi = ["12"]  # Contoh kode provinsi Yogyakarta

    scraper.fetch_kabupaten_kota(kode_provinsi, tahun_options)
    scraper.fetch_apbd_table(kode_provinsi, tahun_options, periode_options)
