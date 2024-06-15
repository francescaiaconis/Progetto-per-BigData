#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min, max, avg, first, last,round

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

# Create Spark session
spark = SparkSession.builder.appName("job1_sql").getOrCreate()

# Read input data from file
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

# Extract year from date and select necessary columns
df = df.withColumn("year", year(col("date"))).select("ticker", "year", "name", "close", "low", "high", "volume")

# Calculate statistics for each ticker and year
stats_df = df.groupBy("ticker", "name", "year").agg(
    round(((last("close") - first("close")) / first("close") * 100), 2).alias("percent_change"),
    min("low").alias("min_price"),
    max("high").alias("max_price"),
    avg("volume").alias("avg_volume")
)

# Order the result by ticker and year
results_df = stats_df.orderBy("ticker", "year")

# Format the results and save to the specified output path
results_df.write.option("header", "true").csv(output_path)

# Stop Spark session
spark.stop()
