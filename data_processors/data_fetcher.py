#!/usr/bin/env python
# encoding: utf-8

# 2013-08-18
# @vollerts

# library imports
import datetime, time
from urllib import urlopen
import numpy as np

# Schema definition
schema = np.dtype({'names':['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjclose'], 'formats':['S8', 'M8[D]', float, float, float, float, float, float]})

def get_yahoo_data(symbol, startdate=None, enddate=None, period='d', datefmt="%Y-%m-%d"):

# API defnition
# symbol = string
# startdate = string
# enddate = string
# period = string
# datefmt = string
# returns numpy array numpy.dtype({'names':['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjclose'], 'formats':['S8', 'M8[D]', float, float, float, float, float, float]})
    
    todaydate = datetime.date(*time.localtime()[:3])
    yesterdate = todaydate - datetime.timedelta(1)
    lastyeardate = todaydate - datetime.timedelta(365)
    
    if startdate is None:
        startdate = lastyeardate
    else:
        startdate = datetime.datetime.strptime(startdate, datefmt)
    
    if enddate is None:
        enddate = yesterdate
    else:
        enddate = datetime.datetime.strptime(enddate, datefmt)
    
    # definition of Yahoo! API call
    url = "http://ichart.finance.yahoo.com/table.csv?s=%s&a=%d&b=%d&c=%d&"\
              "d=%d&e=%d&f=%d&y=0&g=%s&ignore=.csv" % (symbol,
              startdate.month-1, startdate.day, startdate.year,
              enddate.month-1, enddate.day, enddate.year, period)
    
    filehandle = urlopen(url)
    lines = filehandle.readlines()
    
    # array holding te returned Yahoo! data
    data = []
    
    for line in lines[1:]:
        
        # definition of seperator
        items = line.strip().split(',')
        
        # catch error if there is an item with less than 7 properties
        if len(items)!=7:
            continue
        
        dt = items[0]
        opn, high, low, close, volume, adjclose = [float(x) for x in items[1:7]]
        data.append((symbol, dt, opn, high, low, close, volume, adjclose))
        
    npdata = np.array(data, dtype=schema)
    return npdata
    
if __name__ == '__main__':
	main()

