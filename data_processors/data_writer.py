#!/usr/bin/env python
# encoding: utf-8

# 2013-08-18
# @vollerts

# library imports
import os
import sys
import datetime, time
import csv
import numpy as np
import sqlite3
import data_fetcher 

def create_db(filename="test.db"):
    
    if os.path.exists(filename):
        raise IOError
    
    conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    # definition stocks table
    conn.execute('''CREATE TABLE stocks (symbol text, date datetime, open float, high float, low float, close float, volume float, adjclose float)''')
    conn.execute('''CREATE UNIQUE INDEX stock_idx ON stocks (symbol, date)''')
    # definition symbol_list table
    conn.execute('''CREATE TABLE symbol_list (symbol text, startdate datetime, enddate datetime, entries long)''')
    conn.execute('''CREATE UNIQUE INDEX symbols_idx ON symbol_list (symbol)''')
    conn.commit()
    conn.close()
    return

def save_to_db(data, dbfilename="data_sources/stocks.db"):

    if not os.path.exists(dbfilename):
        create_db(dbfilename)
    conn = sqlite3.connect(dbfilename,
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()

    # try insert 
    try:
        sql = "INSERT INTO stocks (symbol, date, open, high, low, close, volume, adjclose) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
        
        # "executemany" creates intermediate cursor object and calls
        c.executemany(sql, data.tolist())
    except sqlite3.IntegrityError:
        pass

    conn.commit()
    change_count = conn.total_changes
    c.close()
    conn.close()
    return change_count

def load_from_db(symbol, startdate, enddate, dbfilename="data_sources/stocks.db"):
    
    # "timetuple" returns http://docs.python.org/2/library/time.html#time.struct_time
    dt = np.dtype('M8[D]')
    startdate = time.mktime(np.array([startdate], dtype=dt).tolist()[0].timetuple())
    enddate = time.mktime(np.array([enddate], dtype=dt).tolist()[0].timetuple())
    
    conn = sqlite3.connect(dbfilename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    sql = "SELECT symbol, date as 'date [datetime]', open, high, low, " \
          "close, volume, adjclose from stocks where symbol='%s' and " \
          "date>=%s and  date<=%s" % (symbol, startdate, enddate)
    qry = conn.execute(sql)
    recs = qry.fetchall()

    table = np.array(recs, dtype=data_fetcher.schema)
    
    return table
    
def populate_db(symbols, startdate, enddate, dbfilename):

    # initiation of coutners
    save_count = 0
    rec_count = 0

    if isinstance(symbols, str):
        reader = csv.reader(open(symbols))
        
        symbolset = set()
        badchars = ["/", ":", "^", "%", "\\"]

        for line in reader:
    
            symb = line[0]
            for itm in badchars:
                symb = symb.replace(itm, "-")
                symbolset.add(symb.strip())
        symbollist = list(symbolset)
    else:
        symbollist = set(symbols)
    
    tot = float(len(symbollist))
    count=0.0
    print "loading data"
    for symbol in list(symbollist):
        data = data_fetcher.get_yahoo_data(symbol, startdate, enddate)
        num_saved = save_to_db(data, dbfilename)
        count+=1.0
        if num_saved:
            save_count+=1
            rec_count+=num_saved
        print symbol + "",
        # write everything in buffer to cmd
        sys.stdout.flush()

    print "Saved %s records for %s out of %s symbols" % (rec_count, save_count, len(symbollist))

    populate_symbol_list(dbfilename)
    
def symbol_exists(symbol, dbfilename="data_sources/stocks.db"):
    conn = sqlite3.connect(dbfilename, 
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    sql = "SELECT symbol, date as 'date [datetime]' from stocks where symbol='%s';" % (symbol)
    qry = conn.execute(sql)
    recs = qry.fetchall()
    schema = np.dtype({'names':['symbol', 'date'],
                       'formats':['S8', 'M8[D]']})
    table = np.array(recs, dtype=schema)

    startdate = np.datetime64(table['date'][0])
    enddate = np.datetime64(table['date'][-1])
    return len(table), startdate, enddate
    
def all_symbols(dbfilename="data_sources/stocks.db"):
    conn = sqlite3.connect(dbfilename, 
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    sql = "SELECT DISTINCT symbol from stocks;"
    qry = conn.execute(sql)
    recs = qry.fetchall()
    reclist = [list(rec)[0] for rec in recs]
    return reclist

def load_symbols_from_table(dbfilename="data_sources/stocks.db"):
    conn = sqlite3.connect(dbfilename,
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    sql = "SELECT symbol, startdate as 'startdate [datetime]', enddate as 'enddate [datetime]', entries from symbol_list;"
    qry = conn.execute(sql)
    recs = qry.fetchall()
    dt = np.dtype({'names':['symbol', 'startdate', 'enddate', 'entries'],
                   'formats':['S8', 'M8[D]', 'M8[D]', long]})
    return np.array(recs, dtype=dt)


def populate_symbol_list(dbfilename="data_sources/stocks.db", symbols=None):
    if not os.path.exists(dbfilename):
        create_db(dbfilename)

    if symbols is None:
        symbols = all_symbols(dbfilename=dbfilename)

    dt = np.dtype({'names':['symbol', 'startdate', 'enddate', 'entries'],
                   'formats':['S8', 'M8[D]', 'M8[D]', long]})
    data = []

    for symbol in symbols:
        entries, startdate, enddate = symbol_exists(symbol, dbfilename=dbfilename)
        data.append((symbol, startdate, enddate, entries))

    data = np.array(data, dtype=dt)

    conn = sqlite3.connect(dbfilename,
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()

    try:
        sql = "INSERT INTO symbol_list (symbol, startdate, enddate, entries) VALUES (?, ?, ?, ?);"

        c.executemany(sql, data.tolist())
    except sqlite3.IntegrityError:
        pass

    conn.commit()
    change_count = conn.total_changes
    c.close()
    conn.close()
    return change_count
       
def main():
    pass

if __name__ == '__main__':
    main()

#### EOF ##################################################################
