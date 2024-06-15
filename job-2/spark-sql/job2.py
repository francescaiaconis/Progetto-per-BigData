#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, max, sum, avg, round, expr, first

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

# Create Spark session
spark = SparkSession.builder.appName("job2_sql").getOrCreate()

# Read input data from file
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

# Extract year from date and select necessary columns
df = df.withColumn("year", year(col("date"))).select("sector", "industry", "year", "ticker", "open", "close", "volume")

# Calculate percentage change
df = df.withColumn("percent_change", expr("((close - open) / open) * 100"))

# Aggregate data by (sector, industry, year)
aggregated_df = df.groupBy("sector", "industry", "year").agg(
    sum("open").alias("sum_open_price"),
    sum("close").alias("sum_close_price"),
    sum("volume").alias("sum_volume"),
    max("percent_change").alias("max_percent_change"),
    first("ticker").alias("ticker_max_change"),
    max("volume").alias("max_volume"),
    first("ticker").alias("ticker_max_volume")
)

# Calculate industry percentage change
aggregated_df = aggregated_df.withColumn("industry_percent_change", expr(
    "((sum_close_price - sum_open_price) / sum_open_price) * 100"
)).withColumn("max_percent_change", round("max_percent_change", 2))

# Sort the results by industry percentage change in descending order
sorted_df = aggregated_df.orderBy(col("industry_percent_change").desc())

# Select the required columns
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

# Save the results to the specified output path
result_df.write.option("header", "true").csv(output_path)

# Stop Spark session
spark.stop()
