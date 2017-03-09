'''
This module will import the historical options data into the Azure Tables Storage
The historical options data was downloaded from a paid service.  It comprises
a separate subfolder for each month of the year 2016.  For each month subfolder,
there are separate csv files, one for each trading day of the month.

This program runs as a consold program and processes a month folder.  It retrieves
a list of all files in the subfolder, with each file containing one day's worth
of options history data. 

For each file, it reads the data in csv format and writes the data to the Azure table
storage.

Note that we'll also need to construct the historical data for the underlying prices
of each options symbol.  This is a future enhancement, but will need to be done
before we can begin analyzing our data.

todo:

still working on the bulk import.  We have it down to six hours per file for local storage.
this may be less for azure tables.  we also can run multiple concurrent processes.
need to make changes for daily production - change the config file to cloud tables.
then change the warning level of the log file to attempt to suppress some more of the
garbage cluttering up our log files.

Need to create some sort of tracking configuration by writing results of the import
(daily and historical) to the arvixie ms sql database so we can see what's happening.


'''

import os
import csv
import string 
import database as db
import logging as lg
import datetime as dt
import dateHashEpoch as dhe

print("Started...")

currentDate = dt.date.today()  
subfolderMonth = 'C:/Users/rwreagan/Documents/HistoricalOptionsDataDownloads/bb_2016_July'


dhe.dateHashEpoch().createHistoricalLoadDateHashMap()

# get a list of all files in the subfolder
fileNames = os.listdir(subfolderMonth)
fileNumber = 0

for filename in fileNames:
    # for this filename, import the data and write it to Azure
    rows = []
    optionDate = str.replace(filename,'bb_options_','').replace('.csv','')
    lg.basicConfig(filename=subfolderMonth + '/import' +optionDate + '.log', 
                   format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=lg.error)
    lg.error('Import started...')

    rowNumber = 0
    fileNumber += 1
    if fileNumber > 1:
        break
    print(filename)
   
    with open(subfolderMonth + '/' + filename) as f:
        print('Started processing filename: ' + filename)
        lg.error('Started processing filename: ' + filename)
        recordsRead = 0
        recordsWrittenForFile = 0
        recordsWrittenForSymbol = 0
        recordsNotWritten = 0
        symbolNumber = 0
        symbol = ''
        previousSymbol = ''
        csv_f = csv.reader(f)
        # the following for loop processes all rows in a given file:
        for row in csv_f:
            recordsRead += 1
            rowNumber += 1
            symbol = row[0]

            #if symbolNumber > 1:
            #    break  # for development purposes only - remove for production

            if symbol!='UnderlyingSymbol':  # skip the heading row
                if symbol == previousSymbol:
                    rows.append(row)
                    if len(rows) == 100:
                        db.insert_historical_option_data(rows)
                        recordsWrittenForFile += len(rows)
                        recordsWrittenForSymbol += len(rows)
                        rows = []  # reset our batch
                else:   # we're starting a new symbol, so it must have its own batch for partition key restrictions
                    if len(rows) > 0:
                        db.insert_historical_option_data(rows)
                        recordsWrittenForFile += len(rows)
                        recordsWrittenForSymbol += len(rows)
                        print('Records written for symbol ' + previousSymbol + ' is ' + str(recordsWrittenForSymbol))
                        lg.error('Records written for symbol ' + previousSymbol + ' is ' + str(recordsWrittenForSymbol))
                        recordsWrittenForSymbol = 0
                    rows = []  # reset our batch
                    rows.append(row)
                    previousSymbol = symbol
                    symbolNumber += 1

        # if there still are remaining rows in the batch that haven't been inserted, add them here.
        if len(rows) > 0:
            db.insert_historical_option_data(rows)
            rows = []  # reset our batch
            numberRowsInBatch = 0

        lg.error('file imported with ' + str(recordsWrittenForFile) + ' total records')
        print('file imported')


