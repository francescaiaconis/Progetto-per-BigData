#!/usr/bin/env python3

import argparse
from pyspark import SparkConf, SparkContext
from datetime import datetime



# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

# Create Spark context
conf = SparkConf().setAppName("job1")
sc = SparkContext(conf=conf)

# Read input data from file
lines_RDD = sc.textFile(input_file)


# Skip header and parse lines
header = lines_RDD.first()
# Extract and convert necessary fields
parsed_RDD = lines_RDD.filter(lambda line: line != header).map(lambda line: (
    line.strip().split(','),  # Split by comma delimiter
    datetime.strptime(line.strip().split(',')[6], '%Y-%m-%d')  # Extract and parse date
))
# Combine data by ticker and year
stock_data_RDD = parsed_RDD.map(lambda x: (
    (x[0][0], x[1].year),  # (Ticker, Year)
    (x[0][8],  # Name (nome dell'azienda)
     float(x[0][2]),  # Close price (prezzo di chiusura)
     float(x[0][3]),  # Low price (prezzo minimo)
     float(x[0][4]),  # High price (prezzo massimo)
     float(x[0][5]),  # Volume (numero di transazioni)
     x[1]  # Date (data)
    )
)).groupByKey()

# Calculate statistics for each ticker and year
def calculate_year_stats(year_data):
    year_data_list = list(year_data)
    name = year_data_list[0][0]

    # Sort data by date to ensure chronological order
    sorted_data = sorted(year_data_list, key=lambda x: x[5])

    # Extract relevant data
    close_prices = [entry[1] for entry in sorted_data]
    low_prices = [entry[2] for entry in sorted_data]
    high_prices = [entry[3] for entry in sorted_data]
    volumes = [entry[4] for entry in sorted_data]

    # Calculate statistics
    start_close = close_prices[0]
    end_close = close_prices[-1]
    percent_change = ((end_close - start_close) / start_close) * 100 if start_close != 0 else 0
    min_price = min(low_prices)
    max_price = max(high_prices)
    avg_volume = sum(volumes) / len(volumes) if volumes else 0

    return {
        'year': sorted_data[0][5].year,
        'percent_change': round(percent_change, 2),
        'min_price': min_price,
        'max_price': max_price,
        'avg_volume': round(avg_volume, 2)
    }
# Group data by ticker
ticker_data_RDD = stock_data_RDD.map(lambda x: (x[0][0], (x[0][1], x[1])))  # (Ticker, (Year, Data))
ticker_grouped_RDD = ticker_data_RDD.groupByKey().mapValues(list)

# Calculate statistics for each year and aggregate by ticker
stats_RDD = ticker_grouped_RDD.mapValues(lambda years: {
    'name': years[0][1][0][0],  # Use the first name found
    'years': [calculate_year_stats(year_data) for _, year_data in years]
})

# Format the results for saving
results_RDD = stats_RDD.map(lambda x: (
    x[0],  # Ticker
    x[1]['name'],  # Name
    x[1]['years']  # List of yearly statistics
))

# Save the results to the specified output path
results_RDD.map(lambda x: f"{x[0]}\t{x[1]}\t" + "\t".join([f"{year['year']}:{year['percent_change']}:{year['min_price']}:{year['max_price']}:{year['avg_volume']}" for year in x[2]]))
results_RDD.saveAsTextFile(output_path)

# Stop Spark context
sc.stop()