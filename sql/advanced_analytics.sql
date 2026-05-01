-- 1. Cumulative Spending per User (Running Total)
-- Menggunakan Window Function SUM() dengan PARTITION BY untuk melihat 
-- tren pengeluaran kumulatif setiap pengguna seiring berjalannya waktu.
SELECT 
    userid,
    date,
    productid,
    total_price,
    SUM(total_price) OVER (
        PARTITION BY userid 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_spend
FROM 
    orders
ORDER BY 
    userid, date;


-- 2. Top Selling Products Ranking (CTE & DENSE_RANK)
-- Menggunakan Common Table Expression (CTE) untuk agregasi, lalu memberikan 
-- peringkat produk berdasarkan total pendapatan tertinggi.
WITH ProductMetrics AS (
    SELECT 
        productid,
        SUM(quantity) as total_items_sold,
        SUM(total_price) as total_revenue
    FROM 
        orders
    GROUP BY 
        productid
)
SELECT 
    productid,
    total_items_sold,
    total_revenue,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
FROM 
    ProductMetrics;


-- 3. Detecting Anomalies: Orders Above User's Average (AVG Window Function)
-- Mengidentifikasi transaksi spesifik yang nilai harganya di atas 
-- rata-rata pengeluaran historis pengguna tersebut.
WITH UserAverages AS (
    SELECT 
        id as order_id,
        userid,
        date,
        total_price,
        AVG(total_price) OVER (PARTITION BY userid) as avg_user_spend
    FROM 
        orders
)
SELECT 
    order_id,
    userid,
    date,
    total_price,
    ROUND(avg_user_spend, 2) as avg_user_spend,
    ROUND((total_price - avg_user_spend), 2) as spend_difference
FROM 
    UserAverages
WHERE 
    total_price > avg_user_spend;