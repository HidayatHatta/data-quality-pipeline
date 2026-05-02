import pandas as pd
from sqlalchemy import create_engine

def load_dwh(processed_paths):
    print("Memulai proses pemuatan data ke Data Warehouse PostgreSQL...")
    
    # Membuat engine koneksi ke PostgreSQL di dalam container Airflow
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    
    # Aturan Star Schema: Dimensi dimuat lebih dulu, Fakta dimuat terakhir
    load_order = ['dim_users', 'dim_products', 'dim_date', 'fact_sales']
    
    for table_name in load_order:
        if table_name in processed_paths:
            file_path = processed_paths[table_name]
            print(f"Membaca file {file_path} untuk tabel {table_name}...")
            
            # Memuat data dari CSV hasil transformasi
            df = pd.read_csv(file_path)
            
            # Menyimpan data ke tabel PostgreSQL
            # Catatan: Kita menggunakan if_exists='replace' agar Anda bisa menjalankan 
            # pipeline ini berkali-kali tanpa error duplikasi selama masa pengujian (idempotent).
            # Di level production, ini biasanya diganti menjadi 'append' dengan metode UPSERT.
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            
            print(f"-> Tabel {table_name} berhasil dimuat ({len(df)} baris).")
        else:
            print(f"Peringatan: File path untuk tabel {table_name} tidak ditemukan dalam XCom.")
            
    print("Seluruh proses pemuatan ke Data Warehouse telah selesai!")
    return True