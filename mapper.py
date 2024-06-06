#!/usr/bin/env python3
import sys
from datetime import datetime

next(sys.stdin)

for line in sys.stdin:
    row = line.strip().split(',')
    ticker = row[0]
    name=row[8]
    close = row[2]
    low = row[3]
    high = row[4]
    volume = row[5]
    date = row[6]
    year = datetime.strptime(date, '%Y-%m-%d').year

    print(f"{ticker}\t{name}\t{year}\t{close}\t{low}\t{high}\t{volume}\t{date}")

