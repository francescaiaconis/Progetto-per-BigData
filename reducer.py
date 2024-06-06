#!/usr/bin/env python3
import sys
from collections import defaultdict

def reducer():
    current_ticker = None
    current_year = None
    company_name = None
    prices = []
    lows = []
    highs = []
    volumes = []

    for line in sys.stdin:
        ticker, year, close, low, high, volume, date, name = line.strip().split("\t")
        year = int(year)
        close = float(close)
        low = float(low)
        high = float(high)
        volume = int(volume)

        if current_ticker != ticker or current_year != year:
            if current_ticker is not None:
                output_stats(current_ticker, current_year, company_name, prices, lows, highs, volumes)
            
            current_ticker = ticker
            current_year = year
            company_name = name
            prices = []
            lows = []
            highs = []
            volumes = []

        prices.append(close)
        lows.append(low)
        highs.append(high)
        volumes.append(volume)

    if current_ticker is not None:
        output_stats(current_ticker, current_year, company_name, prices, lows, highs, volumes)

def output_stats(ticker, year, name, prices, lows, highs, volumes):
    first_close = prices[0]
    last_close = prices[-1]
    percent_change = ((last_close - first_close) / first_close) * 100
    min_price = min(lows)
    max_price = max(highs)
    avg_volume = sum(volumes) / len(volumes)

    print(f"{ticker}\t{name}\t{year}\t{round(percent_change, 2)}\t{min_price}\t{max_price}\t{avg_volume}")

