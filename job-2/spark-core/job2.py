#!/usr/bin/env python3

import argparse

from pyspark import SparkConf, SparkContext
from datetime import datetime


parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

conf = SparkConf().setAppName("Job1")
sc = SparkContext(conf=conf)
# Read input data from file
lines_RDD = sc.textFile(input_file)

# Skip header and parse lines
header = lines_RDD.first()
data_RDD = lines_RDD.filter(lambda line: line != header).map(lambda line: line.split(','))

# Extract necessary fields and convert types
parsed_RDD = data_RDD.map(lambda row: (
    row[9],  # sector 0
    row[10],  # industry 1
    datetime.strptime(row[6], '%Y-%m-%d').year,  # year 2
    row[0],  # ticker 3
    float(row[1]),  # open_price 4 
    float(row[2]),  # close_price 5
    float(row[5])   # volume 6
))

# Aggregate data by (sector, industry, year)
aggregated_RDD = parsed_RDD.map(lambda x: (
    (x[0], x[1], x[2]),  # (sector, industry, year)
    (x[4], x[5], x[6], (x[5] - x[4]) / x[4] * 100 if x[4] != 0 else 0, x[3])  # (open_price, close_price, volume, percentage_change, ticker)
))

# Reduce to combine values
reduced_RDD = aggregated_RDD.reduceByKey(lambda x, y: (
    x[0] + y[0],  # Sum of open prices
    x[1] + y[1],  # Sum of close prices
    x[2] + y[2],  # Sum of volumes
    max(x[3], y[3]),  # Max percentage change
    x[4] if x[3] >= y[3] else y[4],  # Ticker with max percentage change
    max(x[2], y[2]),  # Max volume
    x[4] if x[2] >= y[2] else y[4]   # Ticker with max volume
))


# Aggiunta di un controllo per la lunghezza delle tuple
def ensure_tuple_length(x):
    if len(x) < 7:
        x = x + (None,) * (7 - len(x))
    return x

reduced_RDD = reduced_RDD.mapValues(ensure_tuple_length)

# Assicurati che tutte le tuple abbiano la stessa lunghezza
flattened_RDD = reduced_RDD.map(lambda x: (
    x[0],  # (settore, industria, anno)
    (x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6])
))

# Calcolo delle statistiche
stats_RDD = flattened_RDD.map(lambda record: (
    record[0][0],  # settore
    record[0][1],  # industria
    record[0][2],  # anno
    ((record[1][1] - record[1][0]) / record[1][0]) * 100 if record[1][0] != 0 else 0,  # variazione percentuale dell'industria
    record[1][4],  # ticker con la massima variazione percentuale
    round(record[1][3],2),  # massima variazione percentuale
    record[1][6],  # ticker con il massimo volume
    record[1][5]   # massimo volume
))

# Sort the results by sector and industry percentage change in descending order
stats_RDD = stats_RDD.sortBy(lambda x: (x[0], -x[3]))

# Format the output
#output_RDD = stats_RDD.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}\t{round(x[3], 2)}\t{x[4]}\t{round(x[5], 2)}\t{x[6]}\t{x[7]}")


# Save the results to the specified output path
stats_RDD.saveAsTextFile(output_path)

# Stop the Spark context
#sc.stop()
