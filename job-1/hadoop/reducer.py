#!/usr/bin/env python3
import sys
from collections import defaultdict


def output_stats(data):
    for ticker, ticker_data in data.items():
        name = ticker_data['name']
        for year, year_data in ticker_data['years'].items():
            prices = year_data['prices']

            # Trova il primo e l'ultimo prezzo di chiusura nell'anno
            first_close_date, first_close = min(prices.items())
            last_close_date, last_close = max(prices.items())

            # Calcola la variazione percentuale
            percent_change = ((last_close - first_close) / first_close) * 100

            # Trova il prezzo minimo e massimo, calcola il volume medio
            min_price = min(year_data['lows'])
            max_price = max(year_data['highs'])
            avg_volume = sum(year_data['volumes']) / len(year_data['volumes'])

            # Stampa le statistiche
            print(f"{ticker}\t{name}\t{year}\t{round(percent_change, 2)}\t{min_price}\t{max_price}\t{round(avg_volume, 2)}")

data = {}
for line in sys.stdin:
    ticker, name, year, day, month, close, low, high, volume, date = line.strip().split("\t")
    year = int(year)
    close = float(close)
    low = float(low)
    high = float(high)
    volume = float(volume)
        
    # Aggiorna i dati dell'azione
    if ticker not in data:
        data[ticker] = {'name': name, 'years': {}}
    if year not in data[ticker]['years']:
        data[ticker]['years'][year] = {'lows': [], 'highs': [], 'volumes': [], 'prices': {}}

    # Aggiunge i dati dell'anno corrente
    data[ticker]['years'][year]['prices'][date] = close
    data[ticker]['years'][year]['lows'].append(low)
    data[ticker]['years'][year]['highs'].append(high)
    data[ticker]['years'][year]['volumes'].append(volume)

output_stats(data)



