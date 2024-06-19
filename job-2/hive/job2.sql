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
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- Caricamento dei dati nella tabella stock_data
LOAD DATA INPATH 'hdfs://localhost:9000/input/merged_data.csv' INTO TABLE stock_data;

DROP VIEW IF EXISTS stock_data_with_year;

-- Step 2: Create view with year extracted
CREATE VIEW stock_data_with_year AS
SELECT
    ticker,
    stock_open,
    stock_close,
    volume,
    year(CAST(stock_date AS DATE)) AS stock_year,
    sector,
    industry
FROM stock_data;

DROP TABLE IF EXISTS industry_stats;
CREATE TABLE industry_stats (
    sector STRING,
    industry STRING,
    stock_year INT,
    industry_percentage_change FLOAT,
    max_percentage_ticker STRING,
    max_percentage FLOAT,
    max_volume_ticker STRING,
    max_volume FLOAT
)
STORED AS PARQUET; -- Puoi scegliere il formato appropriato, come PARQUET, per migliorare le prestazioni

-- Step 1: Calcolo della variazione percentuale e altre statistiche per ciascuna azione
INSERT OVERWRITE TABLE industry_stats
SELECT
    sector,
    industry,
    stock_year,
    ROUND((SUM(stock_close) - SUM(stock_open)) / SUM(stock_open) * 100, 2) AS industry_percentage_change,
    FIRST_VALUE(ticker) OVER (PARTITION BY sector, industry, stock_year ORDER BY ((stock_close - stock_open) / stock_open) DESC) AS max_percentage_ticker,
    MAX((stock_close - stock_open) / stock_open * 100) AS max_percentage,
    FIRST_VALUE(ticker) OVER (PARTITION BY sector, industry, stock_year ORDER BY volume DESC) AS max_volume_ticker,
    MAX(volume) AS max_volume
FROM stock_data_with_year
GROUP BY sector, industry, stock_year, ticker, stock_open, stock_close, volume;

DROP TABLE IF EXISTS final_report;
CREATE TABLE final_report (
    sector STRING,
    industry STRING,
    stock_year INT,
    industry_percentage_change FLOAT,
    max_percentage_ticker STRING,
    max_percentage FLOAT,
    max_volume_ticker STRING,
    max_volume FLOAT
)
STORED AS PARQUET; -- Puoi scegliere il formato appropriato, come PARQUET, per migliorare le prestazioni


-- Step 2: Ordinamento dei risultati per variazione percentuale decrescente
INSERT OVERWRITE TABLE final_report
SELECT
    sector,
    industry,
    stock_year,
    industry_percentage_change,
    max_percentage_ticker,
    max_percentage,
    max_volume_ticker,
    max_volume
FROM industry_stats
ORDER BY industry_percentage_change DESC;

SELECT * FROM final_report LIMIT 10;
