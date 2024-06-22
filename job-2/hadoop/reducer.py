#!/usr/bin/env python3
import sys
from collections import defaultdict
dati = defaultdict(lambda: {
    "sector": None,
        "column": defaultdict(lambda: defaultdict(lambda: {
            "industry": None,
            "year": defaultdict(lambda: defaultdict(lambda: {
            'tickers': defaultdict(lambda: {'open': None, 'close': 0, 'volume': 0}),
            'total_open': 0,
            'total_close': 0,
            'total_volume': 0,
            'max_perc': -float('inf'),
            'max_ticker_perc': None,
            'max_volume': 0,
            'max_volume_ticker': None
        }))
    }))
})

for line in sys.stdin:
    sector, industry, year, ticker, open_price, close_price, volume = line.strip().split("\t")
    year = int(year)
    open_price = float(open_price)
    close_price = float(close_price)
    volume = float(volume)
    if sector not in dati:
        dati[sector]["sector"] = {"sector": sector, "column": {}}
    if industry not in dati[sector]["column"]:
        dati[sector]["column"][industry] = {"industry": industry, "year": {}}
    if year not in dati[sector]["column"][industry]["year"]:
        dati[sector]["column"][industry]["year"][year] = {"year": year, "tickers": {}}
    if ticker not in dati[sector]["column"][industry]["year"][year]["tickers"]:
        dati[sector]["column"][industry]["year"][year]["tickers"][ticker] = {'open': open_price, 'close': close_price, 'volume': volume}
    else:
        dati[sector]["column"][industry]["year"][year]["tickers"][ticker]['close'] = close_price
        dati[sector]["column"][industry]["year"][year]["tickers"][ticker]['volume'] += volume
        dati[sector]["column"][industry]["year"][year]['total_open'] = open_price

results = []  
for sector, sector_data in dati.items():
    for industry, industry_years in sector_data["column"].items():
        for year, year_data in industry_years["year"].items():
            dati[sector]["column"][industry]["year"][year]['total_close'] = sum([ticker['close'] for ticker in year_data['tickers'].values()])
            dati[sector]["column"][industry]["year"][year]['total_open'] = sum([ticker['open'] for ticker in year_data['tickers'].values()])
            dati[sector]["column"][industry]["year"][year]['total_volume'] = sum([ticker['volume'] for ticker in year_data['tickers'].values()])
            dati[sector]["column"][industry]["year"][year]['max_perc'] = max([((ticker['close'] - ticker['open']) / ticker['open']) * 100 for ticker in year_data['tickers'].values()])
            dati[sector]["column"][industry]["year"][year]['max_ticker_perc'] = max(year_data['tickers'].items(), key=lambda x: ((x[1]['close'] - x[1]['open']) / x[1]['open']) * 100)[0]
            dati[sector]["column"][industry]["year"][year]['max_volume'] = max([ticker['volume'] for ticker in year_data['tickers'].values()])
            dati[sector]["column"][industry]["year"][year]['max_volume_ticker'] = max(year_data['tickers'].items(), key=lambda x: x[1]['volume'])[0]
            total_open = year_data["total_open"]
            total_close = year_data["total_close"]
            total_volume = year_data["total_volume"]
            industry_perc_change = ((total_close - total_open) / total_open) * 100 if total_open != 0 else 0
            max_perc_ticker = year_data["max_ticker_perce"]
            max_perc = year_data["max_perc"]
            max_volume_ticker = year_data["max_volume_ticker"]
            max_volume = year_data["max_volume"]

            results.append((sector, industry, year, round(industry_perc_change, 2), max_perc_ticker, round(max_perc, 2), max_volume_ticker, max_volume))

sorted_results = sorted(results, key=lambda x: x[3], reverse=True)

for result in sorted_results:
    print(f"{result[0]}\t{result[1]}\t{result[2]}\t{result[3]}\t{result[4]}\t{result[5]}\t{result[6]}\t{result[7]}")