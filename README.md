# End-to-End Data Engineering: Quality Pipeline & Star Schema DWH

Sebuah proyek *Data Engineering* komprehensif yang dirancang untuk mengekstrak data e-commerce dari REST API, melakukan transformasi data (Pandas), memvalidasi kualitas data, dan membangun arsitektur *Data Warehouse* berbasis *Star Schema* di PostgreSQL menggunakan Apache Airflow. Seluruh infrastruktur diorkestrasi menggunakan Docker dan tervalidasi melalui CI/CD pipeline.

## Arsitektur Pipeline

Proyek ini memiliki dua DAG (*Directed Acyclic Graph*) utama yang merepresentasikan alur kerja standar industri:

### 1. Data Quality Pipeline (`advanced_data_pipeline`)
Fokus pada ekstraksi linier dan pembersihan data:
- **Extract**: Menarik data transaksi e-commerce secara dinamis dari [Fake Store API](https://fakestoreapi.com/).
- **Transform**: Normalisasi struktur JSON yang bersarang (*nested data exploding*), penyesuaian zona waktu, dan kalkulasi metrik.
- **Validate**: Memeriksa kualitas data secara otomatis (mendeteksi *null values* dan duplikasi baris).
- **Load**: Memuat data yang telah bersih ke dalam *database* PostgreSQL.

### 2. Star Schema Data Warehouse (`star_schema_dwh_pipeline`)
Fokus pada *Dimensional Modeling* untuk kebutuhan analitik:
- Mengekstrak multientitas (*Users, Products, Carts*) secara terisolasi.
- Memecah data transaksional menjadi tabel dimensi (`dim_users`, `dim_products`, `dim_date`) dan tabel fakta (`fact_sales`).
- Memuat tabel ke PostgreSQL dengan mengedepankan integritas relasional (*Foreign Keys*).

## Business Intelligence & Visualisasi

Tabel *Star Schema* yang telah diproses dihubungkan secara langsung ke **Power BI Desktop** (Mode *Import/DirectQuery*) untuk menghasilkan analitik bisnis yang interaktif.

### Model Relasi (Star Schema)
![Model View](https://raw.githubusercontent.com/HidayatHatta/assets-ml-terapan/main/Model%20View.png)

### Dashboard Analitik
![Report View](https://raw.githubusercontent.com/HidayatHatta/assets-ml-terapan/main/Report%20View.png)

## Advanced SQL & Data Analytics

Proyek ini juga mendemonstrasikan kemampuan *query* analitik tingkat lanjut dan optimasi *database* (dapat dilihat pada direktori `sql/`):
- **Window Functions & CTEs**: Perhitungan *running total*, pemeringkatan produk (`DENSE_RANK`), dan deteksi anomali harga.
- **Performance Tuning**: Implementasi *B-Tree Composite Indexes* untuk mengoptimalkan *query* berbasis filter waktu dan *grouping*.

## Struktur Direktori

```text
data-pipeline-advanced/
├── .github/
│   └── workflows/
│       └── airflow-ci.yml     # CI/CD Pipeline
├── dags/
│   └── pipeline_dag.py        # DAG 1: Data Quality
│   └── dwh_dag.py             # DAG 2: Star Schema
├── scripts/
│   ├── extract.py / extract_dwh.py
│   ├── transform.py / transform_dwh.py
│   ├── validate.py
│   ├── load.py / load_dwh.py
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
