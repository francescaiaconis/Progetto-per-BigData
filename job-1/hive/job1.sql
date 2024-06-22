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

LOAD DATA INPATH 'hdfs://localhost:9000/input/merged_data.csv' INTO TABLE stock_data;

DROP VIEW IF EXISTS stock_data_with_year;
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


DROP table IF EXISTS stock_statistics;
DROP VIEW IF EXISTS stock_statistics;
CREATE VIEW stock_statistics AS
WITH ranked_data AS (
    SELECT
        ticker,
        stock_name,
        stock_year,
        stock_close,
        low,
        high,
        volume,
        stock_date,
        ROW_NUMBER() OVER (PARTITION BY ticker, stock_year ORDER BY stock_date ASC) AS row_num_asc,
        ROW_NUMBER() OVER (PARTITION BY ticker, stock_year ORDER BY stock_date DESC) AS row_num_desc
    FROM stock_data_with_year
)
SELECT
    ticker,
    stock_name,
    stock_year,
    ROUND(((MAX(CASE WHEN row_num_desc = 1 THEN stock_close ELSE NULL END) -
             MAX(CASE WHEN row_num_asc = 1 THEN stock_close ELSE NULL END)) /
            MAX(CASE WHEN row_num_asc = 1 THEN stock_close ELSE NULL END)) * 100, 2) AS percent_change,
    MIN(low) AS min_price,
    MAX(high) AS max_price,
    ROUND(AVG(volume), 2) AS avg_volume
FROM ranked_data
GROUP BY ticker, stock_name, stock_year;

SELECT * FROM stock_statistics LIMIT 10;