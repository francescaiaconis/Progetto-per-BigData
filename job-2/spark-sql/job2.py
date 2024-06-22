#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, max, sum, round, expr, first

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

spark = SparkSession.builder.appName("job2_sql").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

df = df.withColumn("year", year(col("date"))).select("sector", "industry", "year", "ticker", "open", "close", "volume")

df = df.withColumn("percent_change", expr("((close - open) / open) * 100"))

# Aggrega per settore, industria e anno
aggregated_df = df.groupBy("sector", "industry", "year").agg(
    sum("open").alias("sum_open_price"),
    sum("close").alias("sum_close_price"),
    sum("volume").alias("sum_volume"),
    max("percent_change").alias("max_percent_change"),
    first("ticker").alias("ticker_max_change"),
    max("volume").alias("max_volume"),
    first("ticker").alias("ticker_max_volume")
)

# Calcola la variazione percentuale dell'industria
aggregated_df = aggregated_df.withColumn("industry_percent_change", expr(
    "((sum_close_price - sum_open_price) / sum_open_price) * 100"
)).withColumn("max_percent_change", round("max_percent_change", 2))

# Ordina per settore e industry_percent_change in ordine decrescente
sorted_df = aggregated_df.orderBy(col("sector"), col("industry_percent_change").desc())

result_df = sorted_df.select(
    "sector",
    "industry",
    "year",
    "industry_percent_change",
    "ticker_max_change",
    "max_percent_change",
    "ticker_max_volume",
    "max_volume"
)

result_df.write.option("header", "true").csv(output_path)

spark.stop()
