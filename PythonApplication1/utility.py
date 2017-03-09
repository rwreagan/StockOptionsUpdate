import datetime
import yahoofinance as yf
import optionChain as oc
from datetime import timedelta
import pytz
import config
from azure.storage.table import EntityProperty, EdmType
from azure.storage import CloudStorageAccount

TABLE_NAME_SYMBOLS = "Symbols" # name of Azure table for list of S&P 500 stock symbols
TABLE_NAME_STOCKS = "Stocks" # name of Azure table for stock historical data
TABLE_NAME_OPTIONS = "Options" # name of the Azure table for option historical data

TABLE_NAME_AZURETEST = "AzureTest" # name of the Azure table for option historical data

"""
Convert the date from the format in which it appears in the yahoo finance api
to a format that can be used for a date in Python.  The yahoo finance version
is an integer value representing the number of seconds since Jan 1, 1970
"""
def epoch_to_date(seconds):
    epoch_date = datetime.date(1970,1,1)
    return  epoch_date + timedelta(seconds = seconds)


"""
Make an api call to retrieve options trading data for symbol 'spy'.
This is the etf for the S&P 500.  If the market is open (i.e., it's
not a weekend or bank holiday), then the function will see the trading
data for 'spy' and return True, indicating that the market is open today.

Note: we also can look at the attribute "marketState".  The set of attributes
varies depending on whether the json was generated while the market was open
or closed.  Both have an attribute "marketState", but the values differ
depending on open or closed.  We'll need to investigate further to see
if there are any other values.
"""
def is_market_closed_today(currentDate, currentDateAzure):
    symbol = 'spy' 
    try:
        jsonData = yf.retrieve_option_expiration_dates(symbol)
        if len(jsonData['optionChain']['result'][0]['options']) > 0:
            optionChain =  oc.OptionChain(jsonData, symbol, currentDate, currentDateAzure)
            mostRecentTradeDate = jsonData['optionChain']['result'][0]['quote']['postMarketTime'] 
            return epoch_to_date(mostRecentTradeDate)!=currentDate
        
    except KeyError as err:
        # If the market is open when this call is made, then the json results will not include
        # an attribute for "postMarketTime".  This will generate a KeyError and bring us to 
        # this chunk of code.  We'll assume we got here because the market is currently open,
        # and return a result of False to indicate that the market is not closed
        return False


def date_for_azure(dt):
    # receives a datetime object
    # returns the object in a format that will go into Azure as a datetime object

    # first, strip the time component from the datetime to ensure dates of the same day will match
    dtNoTime = datetime.datetime(dt.year, dt.month, dt.day)
    # add the timezone component required for an "aware" date object
    dtAware = pytz.timezone('US/Eastern').localize(dtNoTime)

    # now cast this as an EntityProperty for use in the Azure entity object to be passed to the table update
    # Azure Table Storage requires that the date have a time zone component (i.e., is "aware")
    ep = EntityProperty(EdmType.DATETIME, dtAware)
    return ep

def date_for_import(stringDate):
    # receives the date in string format from the import of the historical options data
    # returns as (unaware) datetime object (that is, not timezone aware)
    return(datetime.datetime.strptime(stringDate,'%m/%d/%Y'))


def get_azure_account():
    # returns the account needed to access the Azure storage tables
    account = None
    if config.IS_EMULATED:
        account = CloudStorageAccount(is_emulated=True)
    else:
        account_name = config.STORAGE_ACCOUNT_NAME
        account_key = config.STORAGE_ACCOUNT_KEY
        account = CloudStorageAccount(account_name, account_key)
    return account


epochDates = {}

historicalLoadDates = {} # used only for the initial loading of the historical options data