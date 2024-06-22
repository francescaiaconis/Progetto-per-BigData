#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min, max, avg, first, last,round

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

spark = SparkSession.builder.appName("job1_sql").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

df = df.withColumn("year", year(col("date"))).select("ticker", "year", "name", "close", "low", "high", "volume")

stats_df = df.groupBy("ticker", "name", "year").agg(
    round(((last("close") - first("close")) / first("close") * 100), 2).alias("percent_change"),
    min("low").alias("min_price"),
    max("high").alias("max_price"),
    avg("volume").alias("avg_volume")
)

results_df = stats_df.orderBy("ticker", "year")

results_df.write.option("header", "true").csv(output_path)

spark.stop()
