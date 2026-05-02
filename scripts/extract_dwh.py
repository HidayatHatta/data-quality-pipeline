import requests
import pandas as pd
from datetime import datetime
import os

def extract_dwh():
    # Definisikan endpoint dari Fake Store API
    endpoints = {
        'users': 'https://fakestoreapi.com/users',
        'products': 'https://fakestoreapi.com/products',
        'carts': 'https://fakestoreapi.com/carts'
    }
    
    # Dictionary untuk menyimpan lokasi file yang berhasil diunduh
    file_paths = {}
    
    # Generate timestamp yang sama untuk satu batch penarikan data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_dir = "/opt/airflow/data/raw/dwh"
    
    # Pastikan folder penyimpanan raw/dwh tersedia
    os.makedirs(raw_dir, exist_ok=True)

    # Looping untuk mengekstrak setiap endpoint
    for entity, url in endpoints.items():
        try:
            print(f"Memulai ekstraksi data: {entity}...")
            response = requests.get(url)
            response.raise_for_status()  # Mencegah error diam-diam jika API down
            
            data = response.json()
            
            # Normalisasi JSON ke DataFrame
            df = pd.json_normalize(data)
            
            # Tentukan path penyimpanan
            file_path = f"{raw_dir}/{entity}_{timestamp}.csv"
            
            # Simpan ke CSV
            df.to_csv(file_path, index=False)
            file_paths[entity] = file_path
            
            print(f"Data {entity} berhasil disimpan ke {file_path}")
            
        except requests.exceptions.RequestException as e:
            print(f"Error saat mengekstrak data {entity}: {e}")
            raise  # Menghentikan Airflow task jika salah satu ekstraksi gagal

    # Mengembalikan dictionary berisi 3 path file untuk dilempar via XCom
    return file_paths