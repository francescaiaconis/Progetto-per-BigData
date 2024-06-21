-- Creazione della tabella per i dati di input, se necessario
DROP TABLE IF EXISTS stock_data;
CREATE TABLE stock_data (
    ticker STRING,
    stock_open FLOAT,
    stock_close FLOAT,
    low FLOAT,
    high FLOAT,
    volume FLOAT,
    stock_date DATE,
    exchange_name STRING,
    stock_name STRING,
    sector STRING,
    industry STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Caricamento dei dati nella tabella stock_data
LOAD DATA INPATH 'hdfs://localhost:9000/input/merged_data_truck.csv' INTO TABLE stock_data;

-- Step 2: Creazione della vista con l'anno estratto
DROP VIEW IF EXISTS stock_data_with_year;

CREATE VIEW stock_data_with_year AS
SELECT
    ticker,
    stock_open,
    stock_close,
    low,
    high,
    volume,
    year(stock_date) AS stock_year,
    stock_date,
    exchange_name,
    stock_name,
    sector,
    industry
FROM
    stock_data;

    
WITH aggregated_data AS (
    SELECT 
        sector,
        industry,
        stock_year,
        ticker,
        stock_open,
        stock_close,
        SUM(volume) as total_volume
    FROM stock_data_with_year
    GROUP BY sector, industry, stock_year, ticker, stock_open, stock_close
),
industry_totals AS (
    SELECT
        sector,
        industry,
        stock_year,
        SUM(stock_open) as total_open_price,
        SUM(stock_close) as total_close_price,
        SUM(total_volume) as total_volume
    FROM aggregated_data
    GROUP BY sector, industry, stock_year
),
industry_percentage_change AS (
    SELECT
        sector,
        industry,
        stock_year,
        (total_close_price - total_open_price) / total_open_price * 100 as percentage_change
    FROM industry_totals
),
max_percentage_ticker AS (
    SELECT
        sector,
        industry,
        stock_year,
        ticker,
        (stock_close - stock_open) / stock_open * 100 as percentage_change,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, stock_year ORDER BY (stock_close - stock_open) / stock_open * 100 DESC) as rank
    FROM aggregated_data
),
max_volume_ticker AS (
    SELECT
        sector,
        industry,
        stock_year,
        ticker,
        total_volume,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, stock_year ORDER BY total_volume DESC) as rank
    FROM aggregated_data
)
SELECT 
    ipc.sector,
    ipc.industry,
    ipc.stock_year,
    ROUND(ipc.percentage_change, 2) as industry_percentage_change,
    mpt.ticker as max_percentage_ticker,
    ROUND(mpt.percentage_change, 2) as max_percentage,
    mvt.ticker as max_volume_ticker,
    mvt.total_volume
FROM 
    industry_percentage_change ipc
JOIN 
    max_percentage_ticker mpt ON ipc.sector = mpt.sector AND ipc.industry = mpt.industry AND ipc.stock_year = mpt.stock_year AND mpt.rank = 1
JOIN 
    max_volume_ticker mvt ON ipc.sector = mvt.sector AND ipc.industry = mvt.industry AND ipc.stock_year = mvt.stock_year AND mvt.rank = 1
ORDER BY 
    ipc.sector, ipc.industry, ipc.stock_year;