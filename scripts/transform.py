import pandas as pd
import ast
import os

def transform(file_path):
    # Memuat data raw
    df = pd.read_csv(file_path)

    # 1. Menghapus kolom residu database (__v)
    if '__v' in df.columns:
        df = df.drop(columns=['__v'])

    # 2. Standarisasi nama kolom ke huruf kecil
    df.columns = [col.lower() for col in df.columns]

    # 3. Parsing kolom 'products'
    # Kolom ini masuk sebagai string "[{'productId':...}]", kita ubah kembali ke list asli
    df['products'] = df['products'].apply(ast.literal_eval)

    # 4. Data Exploding
    # Mengubah satu baris keranjang menjadi beberapa baris berdasarkan jumlah jenis produk
    # Ini sangat penting untuk analisis data granular di SQL
    df = df.explode('products').reset_index(drop=True)

    # 5. Normalisasi kolom products (Extract productId dan quantity)
    products_info = pd.json_normalize(df['products'])
    df = pd.concat([df.drop(columns=['products']), products_info], axis=1)

    # 6. Konversi tipe data tanggal
    # Mengubah string ISO 8601 menjadi objek datetime agar valid di PostgreSQL
    df['date'] = pd.to_datetime(df['date'])

    # 7. Kalkulasi Harga (Contoh: Mengalikan quantity dengan harga flat 15.000)
    # Karena API tidak menyediakan harga produk di endpoint carts
    df['total_price'] = df['quantity'] * 15000

    # Menentukan path output dan memastikan direktori tersedia
    output_path = file_path.replace("raw", "processed")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Menyimpan hasil transformasi
    df.to_csv(output_path, index=False)

    return output_path