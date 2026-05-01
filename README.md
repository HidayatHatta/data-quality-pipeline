# Advanced Data Quality Pipeline with Apache Airflow & Docker

Sebuah proyek _Data Engineering_ end-to-end yang dirancang untuk mengekstrak data e-commerce dari REST API, melakukan transformasi dan normalisasi data menggunakan Pandas, memvalidasi kualitas data, dan memuatnya ke dalam PostgreSQL menggunakan Apache Airflow. Seluruh environment diorkestrasi menggunakan Docker.

## Arsitektur Pipeline

Pipeline (DAG: `advanced_data_pipeline`) terdiri dari empat tahapan utama:

1. **Extract**: Menarik data transaksi e-commerce secara dinamis dari [Fake Store API](https://fakestoreapi.com/).
2. **Transform**:
   - Normalisasi struktur JSON yang bersarang (_nested data exploding_).
   - Penyesuaian format zona waktu (_timestamp timezone formatting_).
   - Penambahan metrik kalkulasi (`total_price`).
3. **Validate**: Memeriksa kualitas data (mendeteksi _null values_ dan duplikasi baris).
4. **Load**: Memuat data yang telah bersih ke dalam sistem _database_ PostgreSQL.

## Advanced SQL & Data Analytics

Proyek ini juga mencakup implementasi query analitik tingkat lanjut dan optimasi database yang dapat dilihat pada direktori `sql/`:

- **Window Functions & CTEs**: Perhitungan _running total_, pemeringkatan produk (`DENSE_RANK`), dan deteksi anomali harga.
- **Performance Tuning**: Implementasi _B-Tree Composite Indexes_ untuk mengoptimalkan _query_ berbasis waktu dan _grouping_, serta _query_ untuk memonitor ukuran indeks dalam PostgreSQL.

## Struktur Direktori

```text
data-pipeline-advanced/
├── dags/
│   └── pipeline_dag.py
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── validate.py
│   ├── load.py
│   └── utils.py
├── sql/
│   ├── advanced_analytics.sql
│   └── performance_tuning.sql
├── data/              # Folder lokal
├── logs/              # Log Airflow
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Prasyarat

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose
- RAM minimal dialokasikan 4GB untuk Docker Desktop (jika menggunakan Windows/Mac)

## Cara Menjalankan Proyek

1. **Clone Repositori**

   ```bash
   git clone <url-repositori-anda>
   cd data-quality-pipeline
   ```

2. **Inisialisasi Lingkungan Airflow & Database**

   ```bash
   # Inisialisasi database
   docker-compose up airflow-init
   ```

3. **Jalankan Layanan**

   ```bash
   docker-compose up -d
   docker-compose run --rm airflow airflow users create     --username admin     --firstname Admin     --lastname User     --role Admin     --email admin@example.com     --password admin
   ```

4. **Akses Airflow Web UI**
   - Buka browser dan navigasikan ke `http://localhost:8081`
   - Kredensial default: `admin` / `admin`
   - Cari DAG `advanced_data_pipeline`, ubah status menjadi **Unpaused**, dan klik **Trigger DAG** untuk memulai eksekusi.
