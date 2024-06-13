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

header = lines_RDD.first()  # Assuming header exists
# Extract and convert necessary fields
parsed_RDD = lines_RDD.filter(lambda line: line != header).map(lambda line: (
    line.strip().split(','),  # Split by tab delimiter
    datetime.strptime(line.strip().split(',')[6], '%Y-%m-%d')  # Extract and parse date
))

# Combine data by ticker
stock_data_RDD = parsed_RDD.flatMap(lambda x: (
    (x[0][0],  # Ticker
        (x[0][1],  # Name
        x[0][2:],  # Year, day, month (remaining fields)
        x[0][5],  # Close price (float)
        x[0][3],  # Low price (float)
        x[0][4],  # High price (float)
        x[0][7],  # Volume (float)
        x[1].year,  # Year (from parsed date)
        x[1].day,  # Day (from parsed date)
        x[1].month  # Month (from parsed date)
        ))
)).groupByKey()

# Calculate statistics for each ticker and year
stats_RDD = stock_data_RDD.mapValues(lambda year_data: {
    'name': year_data[0][0],
    'prices': dict(zip(map(lambda x: x[0], year_data), map(lambda x: x[3], year_data))),  # Create price dictionary
    'lows': list(map(lambda x: x[1], year_data)),  # List of low prices
    'highs': list(map(lambda x: x[2], year_data)),  # List of high prices
    'volumes': list(map(lambda x: x[5], year_data))  # List of volumes
}).mapValues(lambda year_stats: {
    'year': year_stats['prices'].keys()[0][0],  # First element's year for reference
    'percent_change': ((year_stats['prices'].values()[-1] - year_stats['prices'].values()[0]) / year_stats['prices'].values()[0]) * 100,
    'min_price': min(year_stats['lows']),
    'max_price': max(year_stats['highs']),
    'avg_volume': sum(year_stats['volumes']) / len(year_stats['volumes'])
})

# Format and print the results
stats_RDD.foreach(lambda x: print(f"{x[0]}\t{x[1]['name']}\t{x[1]['year']}\t{round(x[1]['percent_change'], 2)}\t{x[1]['min_price']}\t{x[1]['max_price']}\t{round(x[1]['avg_volume'], 2)}"))
stats_RDD.saveAsTextFile(output_path)

# Stop Spark context
sc.stop()
