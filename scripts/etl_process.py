import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def jalankan_etl():
    print("=== [START] Memulai Proses ETL Transjakarta Modular ===")
    
    # 1. EXTRACT: Membaca data mentah dari folder volume Docker
    path_file = '/opt/airflow/data/data_tiket_dummy.csv'
    print(f"Mengunduh data mentah dari: {path_file}")
    df = pd.read_csv(path_file)
    
    # 2. TRANSFORM: Mekanisme Pembersihan Data Kompleks (Production Standard)
    print("Melakukan transformasi dan pembersihan data...")
    
    # A. Pembersihan Data Hilang (Missing Values)
    # Membuang baris jika kolom penting bernilai kosong
    df_clean = df.dropna(subset=['id_tiket', 'status_gate', 'kode_halte'])
    
    # B. Pembersihan Data Anomali / Minus
    # Menyaring saldo terpotong yang tidak masuk akal (minus) akibat eror sistem sistem
    df_clean = df_clean[df_clean['saldo_terpotong'] >= 0]
    
    # C. Standarisasi Teks (Data Standardisation)
    # Mengubah nama/kode halte menjadi huruf besar semua dan menghapus spasi berlebih
    df_clean['kode_halte'] = df_clean['kode_halte'].str.upper().str.strip()
    df_clean['status_gate'] = df_clean['status_gate'].str.upper().str.strip()
    
    # D. Rekayasa Fitur Waktu (Feature Engineering)
    # Mengonversi string waktu menjadi tipe datetime, lalu mengekstrak jam sibuk penumpang
    df_clean['waktu_tap'] = pd.to_datetime(df_clean['waktu_tap'])
    df_clean['jam_tap'] = df_clean['waktu_tap'].dt.hour
    
    # Menentukan kategori jam sibuk (Peak Hour) Transjakarta (06-09 pagi dan 16-19 sore)
    df_clean['is_jam_sibuk'] = df_clean['jam_tap'].apply(
        lambda x: True if (6 <= x <= 9) or (16 <= x <= 19) else False
    )
    
    print(f"Pembersihan Selesai. Total baris valid: {len(df_clean)} dari {len(df)} baris mentah.")
    
    # 3. LOAD: Menggunakan PostgresHook (Aman, Tanpa Bocor Password di GitHub)
    print("Menghubungkan ke database 'postgres-data' via Airflow PostgresHook...")
    
    # Airflow otomatis mengambil kredensial dari koneksi 'postgres_data_conn' di Web UI
    pg_hook = PostgresHook(postgres_conn_id='postgres_data_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Memasukkan hasil data bersih ke dalam database secara bertahap (Append)
    print("Memuat data bersih ke tabel 'transaksi_gate'...")
    df_clean.to_sql(
        name='transaksi_gate', 
        con=engine, 
        if_exists='append', 
        index=False
    )
    
    print("=== [SUCCESS] Proses ETL Selesai! Data Berhasil Disimpan ===")
