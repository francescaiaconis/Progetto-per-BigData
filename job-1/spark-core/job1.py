#!/usr/bin/env python3

import argparse
from pyspark import SparkConf, SparkContext
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

conf = SparkConf().setAppName("job1")
sc = SparkContext(conf=conf)

lines_RDD = sc.textFile(input_file)

header = lines_RDD.first()

parsed_RDD = lines_RDD.filter(lambda line: line != header).map(lambda line: (
    line.strip().split(','),  
    datetime.strptime(line.strip().split(',')[6], '%Y-%m-%d')  
))

stock_data_RDD = parsed_RDD.map(lambda x: (
    (x[0][0], x[1].year),  # (Ticker, Year)
    (x[0][8],  # Name 
     float(x[0][2]),  # Close price 
     float(x[0][3]),  # Low price 
     float(x[0][4]),  # High price 
     float(x[0][5]),  # Volume 
     x[1]  # Data
    )
)).groupByKey()

def calculate_year_stats(year_data):
    year_data_list = list(year_data)
    name = year_data_list[0][0]

    sorted_data = sorted(year_data_list, key=lambda x: x[5])

    close_prices = [entry[1] for entry in sorted_data]
    low_prices = [entry[2] for entry in sorted_data]
    high_prices = [entry[3] for entry in sorted_data]
    volumes = [entry[4] for entry in sorted_data]

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

ticker_data_RDD = stock_data_RDD.map(lambda x: (x[0][0], (x[0][1], list(x[1]))))  # (Ticker, (Year, Data))

ticker_grouped_RDD = ticker_data_RDD.groupByKey().mapValues(list)

stats_RDD = ticker_grouped_RDD.mapValues(lambda years: {
    'name': years[0][1][0][0],  
    'years': [calculate_year_stats(year_data) for _, year_data in years]
})

def format_output(ticker, name, years):
    return "\n".join([
        f"{ticker}\t{name}\t{year['year']}\t{year['percent_change']}\t{year['min_price']}\t{year['max_price']}\t{year['avg_volume']}"
        for year in years
    ])

results_RDD = stats_RDD.flatMap(lambda x: format_output(x[0], x[1]['name'], x[1]['years']).split("\n"))

results_RDD.saveAsTextFile(output_path)

sc.stop()
