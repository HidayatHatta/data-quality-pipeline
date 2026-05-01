-- Tanpa Index, database harus melakukan 'Seq Scan' (memindai seluruh baris) 
-- saat memfilter atau melakukan Window Function pada userid dan date.

-- 1. Composite Index untuk Query Window Function
-- Index ini akan sangat mempercepat query "Cumulative Spending" di atas
-- karena data sudah terurut berdasarkan userid dan date di level disk.
CREATE INDEX idx_orders_userid_date ON orders(userid, date);

-- 2. Index untuk Agregasi Produk
-- Mempercepat proses GROUP BY pada query "Top Selling Products".
CREATE INDEX idx_orders_productid ON orders(productid);

-- Menampilkan statistik ukuran tabel dan index untuk observability
SELECT 
    relname as table_name,
    pg_size_pretty(pg_total_relation_size(relid)) as total_size,
    pg_size_pretty(pg_indexes_size(relid)) as index_size
FROM 
    pg_catalog.pg_statio_user_tables
WHERE 
    relname = 'orders';