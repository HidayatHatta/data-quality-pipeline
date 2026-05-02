import pandas as pd
import ast
import os

def transform_dwh(raw_file_paths):
    print("Memulai proses transformasi Dimensional Modeling (Star Schema)...")
    
    # 1. Load data mentah dari dictionary path
    df_users = pd.read_csv(raw_file_paths['users'])
    df_products = pd.read_csv(raw_file_paths['products'])
    df_carts = pd.read_csv(raw_file_paths['carts'])

    # Siapkan direktori output
    processed_dir = "/opt/airflow/data/processed/dwh"
    os.makedirs(processed_dir, exist_ok=True)
    processed_paths = {}

    # ==========================================
    # 2. TRANSFORMASI DIMENSI (DIMENSION TABLES)
    # ==========================================

    # A. Dimension Table: dim_users
    # Mengambil dan mengganti nama kolom profil pengguna hasil flatten JSON
    dim_users = df_users.copy()
    rename_users_map = {
        'id': 'user_id', 
        'email': 'email',
        'username': 'username',
        'name.firstname': 'first_name', 
        'name.lastname': 'last_name', 
        'address.city': 'city'
    }
    dim_users = dim_users.rename(columns=rename_users_map)
    cols_users = [c for c in rename_users_map.values() if c in dim_users.columns]
    dim_users = dim_users[cols_users]
    
    path_users = f"{processed_dir}/dim_users.csv"
    dim_users.to_csv(path_users, index=False)
    processed_paths['dim_users'] = path_users
    print("-> dim_users berhasil dibuat.")

    # B. Dimension Table: dim_products
    dim_products = df_products[['id', 'title', 'price', 'category']].copy()
    dim_products = dim_products.rename(columns={'id': 'product_id', 'title': 'product_name'})
    
    path_products = f"{processed_dir}/dim_products.csv"
    dim_products.to_csv(path_products, index=False)
    processed_paths['dim_products'] = path_products
    print("-> dim_products berhasil dibuat.")

    # C. Dimension Table: dim_date
    # Mengekstrak tanggal unik dari tabel carts untuk membuat dimensi waktu
    df_carts['date'] = pd.to_datetime(df_carts['date'])
    unique_dates = df_carts['date'].dt.date.unique()
    
    dim_date = pd.DataFrame({'full_date': pd.to_datetime(unique_dates)})
    dim_date['date_id'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int) # Format: 20260501
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['day_name'] = dim_date['full_date'].dt.day_name()
    
    # Reorder kolom agar date_id di depan
    dim_date = dim_date[['date_id', 'full_date', 'year', 'month', 'day', 'day_name']]
    
    path_date = f"{processed_dir}/dim_date.csv"
    dim_date.to_csv(path_date, index=False)
    processed_paths['dim_date'] = path_date
    print("-> dim_date berhasil dibuat.")

    # ==========================================
    # 3. TRANSFORMASI FAKTA (FACT TABLE)
    # ==========================================
    
    # D. Fact Table: fact_sales
    fact_sales = df_carts.copy()
    
    if '__v' in fact_sales.columns:
        fact_sales = fact_sales.drop(columns=['__v'])
        
    # Explode nested JSON pada kolom products
    fact_sales['products'] = fact_sales['products'].apply(ast.literal_eval)
    fact_sales = fact_sales.explode('products').reset_index(drop=True)
    
    products_info = pd.json_normalize(fact_sales['products'])
    fact_sales = pd.concat([fact_sales.drop(columns=['products']), products_info], axis=1)
    
    # Mapping Foreign Keys dan kalkulasi metrik
    fact_sales = fact_sales.rename(columns={'id': 'cart_id', 'userId': 'user_id', 'productId': 'product_id'})
    fact_sales['date_id'] = fact_sales['date'].dt.strftime('%Y%m%d').astype(int) # Hubungkan ke dim_date
    
    # Kalkulasi total_price secara akurat dengan melakukan JOIN/Merge ke dim_products 
    # untuk mendapatkan harga asli produk (bukan harga dummy lagi)
    fact_sales = fact_sales.merge(dim_products[['product_id', 'price']], on='product_id', how='left')
    fact_sales['total_price'] = fact_sales['quantity'] * fact_sales['price']
    
    # Pilih kolom akhir untuk Fact Table (Hanya FK dan Metrik Transaksi)
    fact_sales = fact_sales.rename(columns={'price': 'unit_price'})
    fact_sales = fact_sales[['cart_id', 'user_id', 'product_id', 'date_id', 'quantity', 'unit_price', 'total_price']]
    
    path_sales = f"{processed_dir}/fact_sales.csv"
    fact_sales.to_csv(path_sales, index=False)
    processed_paths['fact_sales'] = path_sales
    print("-> fact_sales berhasil dibuat.")

    return processed_paths