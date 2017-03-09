import datetime  

import azure.common
from azure.storage import CloudStorageAccount
from azure.storage.table import TableService, Entity, TableBatch
"""
This class represents a single option in the option chain.
Options are grouped by expiration date.
"""
class option():
    def __init__(self, symbol, currentDate, currentDateAzure, callPut, expDate, strike, expDateRowKey):
        self.PartitionKey = symbol
        self.RowKey = callPut + currentDate.strftime('%Y%m%d') + expDateRowKey.strftime('%Y%m%d') + str(strike)
        self.symbol = symbol
        self.optionDate = currentDateAzure
        self.callPut = callPut
        self.strike = strike
        self.lastPrice = 0.0
        self.volume = 0 
        self.openInterest = 0 
        self.bid = 0.0
        self.ask = 0.0
        self.expiration = expDate
        self.impliedVolatility = 0.0 
        self.stockPrice = 0.0


