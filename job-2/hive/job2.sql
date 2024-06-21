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

DROP VIEW IF EXISTS stock_data_with_year;

-- Step 2: Create view with year extracted
CREATE VIEW stock_data_with_year AS
SELECT
    ticker,
    stock_open,
    stock_close,
    low,
    high,
    volume,
    year(CAST(stock_date AS DATE)) AS stock_year,
    stock_date,
    exchange_name,
    stock_name,
    sector,
    industry
FROM stock_data;

WITH stock_yearly AS (
    SELECT
        sector,
        industry,
        stock_year,
        ticker,
        FIRST_VALUE(stock_open) OVER (PARTITION BY sector, industry, ticker, stock_year ORDER BY stock_date ASC) AS open_price,
        LAST_VALUE(stock_close) OVER (PARTITION BY sector, industry, ticker, stock_year ORDER BY stock_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price,
        SUM(volume) OVER (PARTITION BY sector, industry, ticker, stock_year) AS total_volume,
        ((LAST_VALUE(stock_close) OVER (PARTITION BY sector, industry, ticker, stock_year ORDER BY stock_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 
        FIRST_VALUE(stock_open) OVER (PARTITION BY sector, industry, ticker, stock_year ORDER BY stock_date ASC)) / 
        FIRST_VALUE(stock_open) OVER (PARTITION BY sector, industry, ticker, stock_year ORDER BY stock_date ASC)) * 100 AS percent_change
    FROM
        stock_data_with_year
),

industry_yearly AS (
    SELECT
        sector,
        industry,
        stock_year,
        SUM(open_price) AS total_open_price,
        SUM(close_price) AS total_close_price,
        SUM(total_volume) AS total_volume
    FROM
        stock_yearly
    GROUP BY
        sector,
        industry,
        stock_year
),

max_changes AS (
    SELECT
        sector,
        industry,
        stock_year,
        ticker AS max_percent_ticker,
        percent_change AS max_percent_change,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, stock_year ORDER BY percent_change DESC) AS rank_change
    FROM
        stock_yearly
),

max_volumes AS (
    SELECT
        sector,
        industry,
        stock_year,
        ticker AS max_volume_ticker,
        total_volume AS max_volume,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, stock_year ORDER BY total_volume DESC) AS rank_volume
    FROM
        stock_yearly
),

ranked_changes AS (
    SELECT
        sector,
        industry,
        stock_year,
        max_percent_ticker,
        max_percent_change
    FROM
        max_changes
    WHERE
        rank_change = 1
),

ranked_volumes AS (
    SELECT
        sector,
        industry,
        stock_year,
        max_volume_ticker,
        max_volume
    FROM
        max_volumes
    WHERE
        rank_volume = 1
)

SELECT
    iy.sector,
    iy.industry,
    iy.stock_year,
    ROUND(((iy.total_close_price - iy.total_open_price) / iy.total_open_price) * 100, 2) AS industry_percent_change,
    rc.max_percent_ticker,
    ROUND(rc.max_percent_change, 2) AS max_percent_change,
    rv.max_volume_ticker,
    rv.max_volume
FROM
    industry_yearly iy
JOIN
    ranked_changes rc
ON
    iy.sector = rc.sector AND iy.industry = rc.industry AND iy.stock_year = rc.stock_year
JOIN
    ranked_volumes rv
ON
    iy.sector = rv.sector AND iy.industry = rv.industry AND iy.stock_year = rv.stock_year
ORDER BY
    iy.sector,
    industry_percent_change DESC;
