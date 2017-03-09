import urllib
import pytz
import pandas as pd 
from bs4 import BeautifulSoup
from datetime import datetime

SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
START = datetime(1900, 1, 1, 0, 0, 0, 0, pytz.utc)
END = datetime.today().utcnow()

'''
Retrieves a list of the option symbols comprising the S&P 500.  The most current list that
is available to us is from the wikipedia web page.  This scrapes the page and
returns the list of symbol tickers.
'''

def scrape_list():
    print('Begin S&P 500 retrieval...')
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(SITE, headers=hdr)
    page = urllib.request.urlopen(req)
    soup = BeautifulSoup(page, "html.parser")

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers
