#!/usr/bin/env python3
import sys
from collections import defaultdict
dati = defaultdict(lambda: {
    "sector": None,
        "statistics": defaultdict(lambda: defaultdict(lambda: {
            "industry": None,
            "year": defaultdict(lambda: defaultdict(lambda: {
            'tickers': defaultdict(lambda: {'open': None, 'close': 0, 'volume': 0}),
            'total_open': 0,
            'total_close': 0,
            'total_volume': 0,
            'max_percentage': -float('inf'),
            'max_ticker_percentage': None,
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
        dati[sector]["sector"] = {"sector": sector, "statistics": {}}
    if industry not in dati[sector]["statistics"]:
        dati[sector]["statistics"][industry] = {"industry": industry, "year": {}}
    if year not in dati[sector]["statistics"][industry]["year"]:
        dati[sector]["statistics"][industry]["year"][year] = {"year": year, "tickers": {}}
    if ticker not in dati[sector]["statistics"][industry]["year"][year]["tickers"]:
        dati[sector]["statistics"][industry]["year"][year]["tickers"][ticker] = {'open': open_price, 'close': close_price, 'volume': volume}
    else:
        dati[sector]["statistics"][industry]["year"][year]["tickers"][ticker]['close'] = close_price
        dati[sector]["statistics"][industry]["year"][year]["tickers"][ticker]['volume'] += volume
        dati[sector]["statistics"][industry]["year"][year]['total_open'] = open_price
    
for sector, sector_data in dati.items():
    for industry, industry_years in sector_data["statistics"].items():
        for year, year_data in industry_years["year"].items():
            dati[sector]["statistics"][industry]["year"][year]['total_close'] = sum([ticker['close'] for ticker in year_data['tickers'].values()])
            dati[sector]["statistics"][industry]["year"][year]['total_open'] = sum([ticker['open'] for ticker in year_data['tickers'].values()])
            dati[sector]["statistics"][industry]["year"][year]['total_volume'] = sum([ticker['volume'] for ticker in year_data['tickers'].values()])
            dati[sector]["statistics"][industry]["year"][year]['max_percentage'] = max([((ticker['close'] - ticker['open']) / ticker['open']) * 100 for ticker in year_data['tickers'].values()])
            dati[sector]["statistics"][industry]["year"][year]['max_ticker_percentage'] = max(year_data['tickers'].items(), key=lambda x: ((x[1]['close'] - x[1]['open']) / x[1]['open']) * 100)[0]
            dati[sector]["statistics"][industry]["year"][year]['max_volume'] = max([ticker['volume'] for ticker in year_data['tickers'].values()])
            dati[sector]["statistics"][industry]["year"][year]['max_volume_ticker'] = max(year_data['tickers'].items(), key=lambda x: x[1]['volume'])[0]
            total_open = year_data["total_open"]
            total_close = year_data["total_close"]
            total_volume = year_data["total_volume"]
            industry_percentage_change = ((total_close - total_open) / total_open) * 100 if total_open != 0 else 0
            max_percentage_ticker = year_data["max_ticker_percentage"]
            max_percentage = year_data["max_percentage"]
            max_volume_ticker = year_data["max_volume_ticker"]
            max_volume = year_data["max_volume"]
            print(f"{sector}\t{industry}\t{year}\t{round(industry_percentage_change, 2)}\t{max_percentage_ticker}\t{round(max_percentage, 2)}\t{max_volume_ticker}\t{max_volume}")
