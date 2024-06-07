#!/usr/bin/env python3
import sys
from datetime import datetime


# Salta l'intestazione
next(sys.stdin)

for line in sys.stdin:
    row = line.strip().split(',')
    ticker = row[0]
    close = row[2]
    open = row[1]
    volume = row[5]
    date = row[6]
    year = datetime.strptime(date, '%Y-%m-%d').year
    industry = row[10]
    sector = row[9]

    print(f"{sector}\t{industry}\t{year}\t{ticker}\t{open}\t{close}\t{volume}")