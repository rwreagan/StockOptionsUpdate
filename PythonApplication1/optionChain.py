import datetime as dt
import stock as s
import option as o
import yahoofinance as yf
import utility as ut
import logging as lg
import database as db
import dateHashEpoch as dhe

"""
This class can be instantiated to create an option chain object.  
It receives, on instantiation, a string of data from the yahoo
finance api call in json format that contains the data.

It parses the data and populates the attributes with the associated
data.  Since the option chain can have multiple entities (one
per expiration date and strike price), it also contains lists
of objects

The json data passed in as the parameter for the creation of the object
contains a list of all expiration dates for this symbol, plus the
option chain data for the first of the expiration dates.

To complete the instatantiation of the object, we still need to make
separate api calls for each of the expiration dates to retrieve the
option chain data for that expiration date
"""

class OptionChain:

    def harvestOptionData(self, jsonOptionData, symbol, callPut, currentDate, currentDateAzure, expiration, stockPrice):
        # receives the json string and callPut value.
        # walks the json tree to parse the data and creates and appends an option object

        # change the following line to use the dictionary to get the date:
        #expDate = ut.epoch_to_date(expiration)
        expDateRowKey = ut.epoch_to_date(expiration)
        try:
            expDate = ut.epochDates[str(expiration)]
        except Exception as err:
            expDate = ut.epoch_to_date(expiration)

        for jsonOption in jsonOptionData: 
            option = o.option(symbol, currentDate, currentDateAzure, callPut, expDate, jsonOption['strike'], expDateRowKey)
            option.lastPrice = jsonOption['lastPrice']
            option.volume = jsonOption['volume']
            option.openInterest = jsonOption['openInterest']
            option.bid = jsonOption['bid']
            option.ask = jsonOption['ask']
            option.impliedVolatility = jsonOption['impliedVolatility']
            option.stockPrice = stockPrice
            self.options.append(option)
            # end of function  


    def __init__(self, jsonData, symbol, currentDate, currentDateAzure):
        # parse the json data and populate the attributes of this object
        if symbol == 'spy':
            return  # don't need to update for the symbol list

        self.stockSymbol = jsonData['optionChain']['result'][0]['underlyingSymbol']
        self.stockBid = jsonData['optionChain']['result'][0]['quote']['bid']
        self.stockAsk = jsonData['optionChain']['result'][0]['quote']['ask']
        try:
            self.stockPrice = jsonData['optionChain']['result'][0]['quote']['postMarketPrice']
        except Exception as err:
            # can occur if the program is run while the market is open.  In this case, the 'postMarketPrice' does not exist in the json
            self.stockPrice = 0.0
        self.options = []

        if len(jsonData['optionChain']['result'][0]['options']) == 0:
            return None

        # while we're here, let's create an entry in the stocks table to show the 
        # stock performance for today.
        stockOpen = jsonData['optionChain']['result'][0]['quote']['regularMarketOpen']
        stockHigh = jsonData['optionChain']['result'][0]['quote']['regularMarketDayHigh']
        stockLow = jsonData['optionChain']['result'][0]['quote']['regularMarketDayLow']
        stockClose = jsonData['optionChain']['result'][0]['quote']['regularMarketPrice']
        stockVolume = jsonData['optionChain']['result'][0]['quote']['regularMarketVolume']
        stock = s.stock(symbol, currentDate, stockOpen, stockHigh, stockLow, stockClose, stockVolume)
        db.update_stock(stock)

        self.expirationDates = jsonData['optionChain']['result'][0]['expirationDates']

        for expiration in self.expirationDates:
            # make api call to get the option chain for each of the expiration dates and add the item to the list
            jsonData = yf.retrieve_option_prices_by_expiration_date(symbol, expiration)
            if len(jsonData['optionChain']['result'][0]['options']) > 0:
                jsonOptionData = jsonData['optionChain']['result'][0]['options'][0]['calls']
                self.harvestOptionData(jsonOptionData, symbol, 'call', currentDate, currentDateAzure, expiration, stockClose)
            
                jsonOptionData = jsonData['optionChain']['result'][0]['options'][0]['puts']
                self.harvestOptionData(jsonOptionData, symbol, 'put', currentDate, currentDateAzure, expiration, stockClose)

        try:
            db.insert_options_azure(self)
            db.write_log_file_entry(symbol, currentDate, len(self.options))

        except Exception as err:
            lg.error('Failed to insert options in azure for symbol ' + symbol)

         
        # https://query2.finance.yahoo.com/v7/finance/options/s?date=1482451200
        # note that we'll need to run the first api call with no query string to get a list of the expiry dates.
        # then run the api call repeatedly, once for each exiry date in the querystring to get the option prices for that expiry

