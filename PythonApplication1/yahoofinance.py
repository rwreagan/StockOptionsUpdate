import urllib.request
import json
import logging as lg

"""
Used to make the (undocumented) api call to finance.yahoo.com
to retrieve the current option prices for the specified stock
trading symbol.

Returns the object containing the option prices for the day for
that security with the specified option expiration date and all strike prices.
"""

def retrieve_option_expiration_dates(symbol):
    try:
        urlData = "https://query2.finance.yahoo.com/v7/finance/options/" + symbol
        webURL = urllib.request.urlopen(urlData)
        data = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        jsonObject = json.loads(data.decode(encoding))
        return jsonObject
    except Exception as err:
        lg.error('Error retrieving expiration dates for symbol ' + symbol)

def retrieve_option_prices_by_expiration_date(symbol, expDate):
    try:
        urlData = "https://query2.finance.yahoo.com/v7/finance/options/" + symbol + "?date=" + str(expDate)
        webURL = urllib.request.urlopen(urlData)
        data = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        jsonObject = json.loads(data.decode(encoding))
        return jsonObject
    except Exception as err:
        lg.error('Error retrieving expiration dates for symbol ' + symbol)
