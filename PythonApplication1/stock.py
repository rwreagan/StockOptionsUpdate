import datetime  

"""
This class represents a single stock in the Stocks table.
"""
class stock:
    def __init__(self, symbol, currentDate, open, high, low, close, volume):
        self.symbol = symbol
        self.date = currentDate
        self.open = open
        self.high = high
        self.low = low
        self.close = close   
        self.volume = volume
