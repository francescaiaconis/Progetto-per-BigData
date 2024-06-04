#!/usr/bin/env python
import pandas as pd

data= pd.read_csv("../../Downloads/historical_stock_prices.csv/merged_data.csv")

results = []
for index, row in data.iterrows():
    key = (row['ticker'], row['name'])
    value = {
        'close': row['close'],
        'low': row['low'],
        'high': row['high'],
        'volume': row['volume'],
        'date': row['date']
    }
    results.append((key, value))
print(results[:5])

