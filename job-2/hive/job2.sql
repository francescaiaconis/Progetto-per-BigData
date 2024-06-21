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
    volume,
    year(stock_date) AS stock_year,
    sector,
    industry
FROM
    stock_data;

-- Calcolo della variazione percentuale per ciascun ticker
DROP VIEW IF EXISTS stock_data_with_percent_change;

CREATE VIEW stock_data_with_percent_change AS
SELECT
    sector,
    industry,
    stock_year,
    ticker,
    stock_open,
    stock_close,
    volume,
    ((stock_close - stock_open) / stock_open) * 100 AS percent_change
FROM
    stock_data_with_year;

-- Aggregazione dei dati per (sector, industry, year)
DROP VIEW IF EXISTS aggregated_data;

CREATE VIEW aggregated_data AS
SELECT
    sector,
    industry,
    stock_year,
    SUM(stock_open) AS sum_open_price,
    SUM(stock_close) AS sum_close_price,
    SUM(volume) AS sum_volume,
    MAX(percent_change) AS max_percent_change,
    MAX(volume) AS max_volume
FROM
    stock_data_with_percent_change
GROUP BY
    sector, industry, stock_year;

-- Determinazione del ticker con la massima variazione percentuale
DROP VIEW IF EXISTS max_percent_ticker;

CREATE VIEW max_percent_ticker AS
SELECT
    sector,
    industry,
    stock_year,
    percent_change,
    ticker,
    ROW_NUMBER() OVER (PARTITION BY sector, industry, stock_year ORDER BY percent_change DESC) AS rank
FROM
    stock_data_with_percent_change;

-- Determinazione del ticker con il massimo volume
DROP VIEW IF EXISTS max_volume_ticker;

CREATE VIEW max_volume_ticker AS
SELECT
    sector,
    industry,
    stock_year,
    total_volume,
    ticker,
    ROW_NUMBER() OVER (PARTITION BY sector, industry, stock_year ORDER BY total_volume DESC) AS rank
FROM
    (
    SELECT
        sector,
        industry,
        stock_year,
        ticker,
        SUM(volume) AS total_volume
    FROM
        stock_data_with_year
    GROUP BY
        sector, industry, stock_year, ticker
    ) t;

-- Calcolo della variazione percentuale dell'industria e unione dei dati finali
SELECT
    ad.sector,
    ad.industry,
    ad.stock_year,
    ROUND(((ad.sum_close_price - ad.sum_open_price) / ad.sum_open_price) * 100, 2) AS industry_percent_change,
    mpt.ticker AS ticker_max_change,
    ROUND(mpt.percent_change, 2) AS max_percent_change,
    mvt.ticker AS ticker_max_volume,
    mvt.total_volume AS max_volume
FROM
    aggregated_data ad
JOIN
    max_percent_ticker mpt ON ad.sector = mpt.sector AND ad.industry = mpt.industry AND ad.stock_year = mpt.stock_year AND mpt.rank = 1
JOIN
    max_volume_ticker mvt ON ad.sector = mvt.sector AND ad.industry = mvt.industry AND ad.stock_year = mvt.stock_year AND mvt.rank = 1
ORDER BY
    ad.sector, ad.industry, ad.stock_year;
