import pypyodbc
import database as db
import utility as ut
import yahoofinance as yf
import optionChain as oc
import csv
import logging as lg
import datetime as dt
import sp500 as sp
import dateHashEpoch as dhe
"""
Retrieves the current day's option prices for all stocks in the S&P 500.
References a datatable in the database that contains the trading symbols 
for the S&P 500 stocks.

For each trading symbol, makes a call to the (undocumented) yahoo finance api
to get the current day's option trading prices.

Parses the trading prices and stores them in a database table for future analysis.

The undocumented finance.yahoo api for the options chain includes what is apparently
the stock price and stock trading symble in addition to the option chain data.  Absent
documentation, we'll assume for our purposes that the stock data we retrieve for each symbol
is an accurate reflection of the true stock trading data.  There is a separate name/value 
pair for the symbol for the stock.  Usually, the stock trading symbol and the option 
trading symbol are the same, but can vary in some cases.  We'll assume that the 
redundant stock symbol in the options chain api response is a reflection of the potential
difference between stock and option symbols for the underlying equity.

The stock bid and ask data is also returned as part of this api call response.  We'll 
use it for now for our analycis purposes, but later will need to investigate to see
if this represents the open or close bid or ask, or perhaps the adjusted close.


Modifications TODO:

    When we move this from running on the local machine to running on as an azure web service,
    we'll need to capture counts and store them in a table somewhere so we can have access
    to the numbers of rows processed and added.  Maybe also have it generate an email to report
    on the status of the job

    Add a routine to create a dictionary mapping dates in the string format from the harvest
    to aware date objects. Then change the population of the date objects to reference the 
    dictionary
"""
currentDate = dt.date.today()  

'''
Next Steps:

Now we have the dictionary of dates to handle the daily harvest.  Need to 
change the harvest program to get the entity property with the aware date
for inclusion into azure from the dictionary instead of calculating it.

Run a test using just one symbol and update the development azure table.
Then compare our storage with reality to ensure it works. 

Then we'll run that version for production today and check for improvements
in the total elapsed time.

After we get that working satisfactorily, we'll add a similar function
for the historical data import to create and use that feature for
#capturing the date values.
'''

lg.basicConfig(filename='C:/Users/rwreagan/Desktop/OptionsLog/optionalytics' + str(currentDate) + '.log', 
               format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=lg.ERROR)

# Note that we log only events that we identify as a logging level of WARNING or greater.  This is because
# if we log at the INFO level, than it reports http data from deep within the bowels of some of the 
# imported code over which we have no control.  This clutters up the log file with extraneous info we don't
# want to see.  The work-around is to just increase the level of logging threshold to suppress these messages.

if currentDate.weekday() == 6:    # update the list of S&P 500 symbols once a week on Sunday (weekday() Sunday==6, Monday==0)
    lg.error('Scraping options list...')
    sector_tickers = sp.scrape_list()
    lg.error('Options list imported')

    lg.error('Updating database with symbols...')
    print('Updating database with symbols...')
    db.update_symbols(sector_tickers)
    lg.error('Symbols updated')
    print('Symbols updated')

currentDateAzure = ut.date_for_azure(currentDate)  # convert this into an object for Azure Table Storage
if ut.is_market_closed_today(currentDate, currentDateAzure):
    lg.error('The market is closed today.  Terminating program...')
    exit()

if (currentDate.weekday()==5) or (currentDate.weekday()==6):
    lg.error('The marked is closed on weekends.  Terminating program...')
    exit()

lg.error(' ')
lg.error('Started')

symbols_list = db.retrieve_symbols()
dhe.dateHashEpoch().createEpochDateHashMap()

i = 0
for entity in symbols_list:
    i += 1
    #if i > 1:
    #    break;
    symbol = str(entity.RowKey).strip()
    #symbol = 'S'
    print(str(i) + '.  symbol: ' + symbol)
    lg.error(str(i) + '. symbol: ' + symbol)
    try:
        jsonData = yf.retrieve_option_expiration_dates(symbol)
        if len(jsonData['optionChain']['result'][0]['options']) > 0:
            optionChain =  oc.OptionChain(jsonData, symbol, currentDate, currentDateAzure)

    except Exception as err:
        print('HTTP Error for: ' + symbol + '. Skipping this symbol')
        lg.error('HTTP Error for: ' + symbol + '. Skipping this symbol')
        # see if we can write this out as a csv for now.  Later, we'll write to the database or Azure Storage
        #with open('C:/Users/rwreagan/Desktop/options.csv', 'w', newline='') as fp:
        #    a = csv.writer(fp, delimiter=',')
        #    for o in optionChain.options:
        #        dataRow = [o.callPut, o.strike, o.bid, o.ask, o.impliedVolatility, o.expiration]
        #        a.writerow(dataRow)    
        
"""
Note: after the first full run of collecting the api data, a handful of the symbols errored out.
On investigation of individual symbols, they were successfully retrieved after the market closed.
Wondering if the error in the api call was just a random fart, or if it has something to do
with the ongoing updates that occur while the market is open.

Further tests and observations are needed.  It may be that we can keep a log of symbols that failed
to retrieve on the first call, and then retry the symbols on the list in subsequent calls at the
end if the program run.  Will need to add logic to accumulate the list of symbols and retry them
if we go this route.
"""      

print("Finished")
lg.error('Finished')
 




























