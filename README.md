# 🚌 TransJakarta Automated ETL Pipeline with Apache Airflow & Docker

[![Apache Airflow](https://shields.io)](https://apache.org)
[![Docker](https://shields.io)](https://docker.com)
[![Pandas](https://shields.io)](https://pydata.org)
[![PostgreSQL](https://shields.io)](https://postgresql.org)

## 📌 Project Overview
Proyek ini mengimplementasikan data pipeline berbasis **ETL (Extract, Transform, Load)** skala produksi untuk mengotomatisasi pemrosesan data transaksi tiket (*tap-in/tap-out*) transportasi umum TransJakarta. 

Arsitektur sistem dibangun secara modular di atas **Linux WSL** menggunakan **Docker Compose** untuk mengorkestrasi Apache Airflow (dengan Celery Executor), Redis, dan dua kluster database PostgreSQL terpisah (satu untuk metadata Airflow, satu untuk *Data Warehouse* utama).

---

## 🏗️ System Architecture & Data Flow

```text
  [ WSL Linux Host ]                      [ Docker Container Network ]
+---------------------+                +---------------------------------+

|                     |                |                                 |
|  /data              |  Vol. Mapping  |  /opt/airflow/data              |
|  (100+ Raw CSV)     |===============>|  (Pandas Extraction)            |
|                     |                |                ||               |
|  /scripts           |                |  (Data Cleaning & Validation)   |
|  (etl_process.py)   |                |                ||               |
|                     |                |                \/               |
|  /sql               |                |  [ Target Database ]            |
|  (DDL & DML)        |                |  postgres-data:5432             |
|                     |                |  (Table: transaksi_gate)        |
+---------------------+                +---------------------------------+
          ||                                           ||
          ================== Managed By =================
                         [ Airflow Scheduler ]
```

1. **Extract**: Airflow memicu fungsi Python via volume untuk membaca data mentah transaksi TransJakarta (`100+ baris data dummy`) berbentuk `.csv` dari direktori virtual.
2. **Transform**: Pandas membersihkan records dari nilai anomali (seperti *missing values* pada `status_gate` dan kegagalan logika pada `saldo_terpotong` yang bernilai minus).
3. **Load**: Data dimuat ke dalam tabel target database PostgreSQL (`postgres-data`) menggunakan mesin pengetikan SQLAlchemy secara berkala.

---

## 📂 Project Structure
```text
project_airflow_transjakarta/
│
├── dags/
│   └── dag_transjakarta.py       # Airflow DAG (Orchestrator Pipeline)
├── data/
│   └── data_tiket_dummy.csv      # 100+ Records Raw Transaction Data
├── scripts/
│   └── etl_process.py            # Core ETL Logic (Pandas & SQLAlchemy)
├── sql/
│   └── create_table_transjakarta.sql # DDL Table & Idempotency Strategy
├── .env                          # Database Credentials & Decoupling Flags
├── .gitignore                    # Git Exclusion Rules
├── Dockerfile                    # Custom Airflow Image Definition
└── docker-compose.yml            # Infrastructure as Code Specification
```

---

## 💻 Configuration & Code Snippets

### 1. Dockerfile Kustom (`Dockerfile`)
Membangun citra (*image*) Airflow kustom dengan melakukan sinkronisasi zona waktu (GMT+7) dan mengemas pustaka tambahan tanpa mengganggu *core image*:
```dockerfile
FROM apache/airflow:2.10.5

ENV TZ=Asia/Jakarta
USER root
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
```

### 2. File SQL Modular (`sql/create_table_transjakarta.sql`)
Memisahkan kueri DDL dari skrip pemrograman utama untuk menjaga modularitas kode:
```sql
CREATE TABLE IF NOT EXISTS transaksi_gate (
    id_tiket VARCHAR(50) PRIMARY KEY,
    id_penumpang VARCHAR(50),
    kode_halte VARCHAR(100),
    waktu_tap TIMESTAMP,
    saldo_terpotong INT,
    status_gate VARCHAR(50)
);

-- Idempotency Safety Valve
TRUNCATE TABLE transaksi_gate;
```

### 3. Skrip Core ETL (`scripts/etl_process.py`)
Mengeksekusi proses manipulasi data mentah ke target kluster operasional data menggunakan kredensial terisolasi:
```python
import pandas as pd
from sqlalchemy import create_engine

def jalankan_etl():
    # Extract
    df = pd.read_csv('/opt/airflow/data/data_tiket_dummy.csv')
    
    # Transform
    df_clean = df.dropna(subset=['status_gate'])
    df_clean = df_clean[df_clean['saldo_terpotong'] >= 0]
    
    # Load to postgres-data cluster
    engine = create_engine('postgresql://postgres-data:postgres-data@postgres-data:5432/postgres-data')
    df_clean.to_sql('transaksi_gate', con=engine, if_exists='append', index=False)
    print("ETL Process Successfully Completed.")
```

---

## 🛠️ Engineering Challenges & Troubleshooting (Interview Bait)

Saat membangun proyek ini, saya sengaja melewati dan merekayasa beberapa kendala sistem untuk menguji pemahaman mendalam tentang *Data Infrastructure*:

### 1. Masalah Sinkronisasi Jembatan Cache WSL & VS Code
* **Dilema**: Terjadi *race condition* visual di mana VS Code (Windows) menolak pembuatan nama file biasa `Dockerfile` dengan peringatan *File Already Exists*, padahal terminal asli Linux (`ls -la`) mengonfirmasi bahwa berkas telah terhapus bersih.
* **Solusi**: Menghentikan proses penguncian sistem (*file locking*) dengan melakukan penghentian paksa (*End Task*) proses *VS Code Server Core* dari Windows Task Manager, melakukan induksi file via terminal murni menggunakan `touch Dockerfile`, dan menginisialisasi ulang ruang kerja lewat perintah `code .`.

### 2. Docker Container vs WSL Host File Permissions (`chmod 777`)
* **Dilema**: Walaupun folder lokal sudah dipetakan dengan benar menggunakan *Docker Volumes*, tugas Airflow terus mengalami kegagalan akibat error *Permission Denied*.
* **Solusi**: Hal ini terjadi karena kontainer Airflow berjalan menggunakan identitas user kustom (`airflow` ID `50000`), sedangkan folder fisik di-host oleh user Linux utama (`yusuf_cahyo`). Masalah ini diselesaikan dengan mengeksekusi perintah kontrol akses `sudo chmod -R 777` guna memastikan kontainer memiliki paspor yang sah untuk melakukan operasi baca/tulis (*read/write*) ke mesin host.

### 3. Kegagalan UniqueViolation Beruntun & Desain Idempotensi
* **Dilema**: Riwayat log mencatat kegagalan task berupa `psycopg2.errors.UniqueViolation` pada run kedua karena skrip Pandas mencoba melakukan *append* berulang pada kolom *Primary Key* (`id_tiket`) yang sama.
* **Solusi**: Mengubah arsitektur agar bersifat **Idempotent**. Saya menyisipkan perintah `TRUNCATE TABLE` tepat setelah kueri `CREATE TABLE` pada hulu aliran kerja SQL. Dengan cara ini, tabel dibersihkan dari *state* lama setiap kali Airflow melakukan *rerun* tugas, menjamin stabilitas data tanpa duplikasi eror.

---

## 🚀 How to Run this Project
1. Pastikan Anda memiliki Docker dan WSL yang aktif di komputer Anda.
2. Buatlah jaringan global Docker terlebih dahulu melalui CLI terminal:
   ```bash
   docker network create airflow_ds_network
   ```
3. Berikan izin akses berkas pada folder proyek Anda di WSL:
   ```bash
   sudo chmod -R 777 dags/ scripts/ data/ sql/
   ```
4. Jalankan perintah komando tunggal berikut untuk merakit gambar kustom dan menghidupkan seluruh infrastruktur di latar belakang:
   ```bash
   docker compose up -d --build
   ```
5. Akses dashboard utama melalui browser Anda di alamat: `http://localhost:8080` (Gunakan kredensial `airflow` / `airflow`).
