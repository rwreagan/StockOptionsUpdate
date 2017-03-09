import sys, os
import pypyodbc
import optionChain as oc
import option as o
import stock as s
import azure.common
from azure.storage import CloudStorageAccount
from azure.storage.table import TableService, Entity, TableBatch, EntityProperty, EdmType
import utility as ut
import datetime as dt
import pytz
import config
import logging as lg

def retrieve_symbols():
    # retrieves the list of option symbols from the MS SQL Server database.  This has been changed
    # to use Azure Table Storage, but is kept here for reference in case we need to later
    # make a call to the older MSSQL database
    #
    # modified to retrieve symbols from the Azure table storage 'Symbols' table
    # Old code to insert / update the table in the MSSQL database.  This code has been replaced
    ###with pypyodbc.connect('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') as connection:

    ###    # retrieve the cursor
    ###    cursor = connection.cursor()
    ###    cursor.execute("Select Symbol from Symbols")
    ###    rows = cursor.fetchall()

    ###    #connection.close() # connection is automatically closed when using the
    ###    #"with" block
    ###    return rows

    account_name = config.STORAGE_ACCOUNT_NAME
    account_key = config.STORAGE_ACCOUNT_KEY
    account = CloudStorageAccount(account_name, account_key)

    table_service = None
    try:
        table_service = account.create_table_service()
        symbols_list = table_service.query_entities(ut.TABLE_NAME_SYMBOLS, filter="PartitionKey eq 'Symbol'")
        return symbols_list

    except Exception as e:
        print('Error occurred in the sample. If you are using the emulator, please make sure the emulator is running.', e)

def write_log_file_entry(symbol, currentDate, rowCount):
    # writes data to the log file in the MSSQL database logfile table
    with pypyodbc.connect('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') as connection:
        cursor = connection.cursor()
        param_list = [symbol, currentDate, rowCount]
        try:
            cursor.execute("insert into LogFile(Symbol, OptionDate, NumberRows) values (?,?,?)",param_list)
        except Exception as err:
            pass
        connection.commit()


def update_symbols(sector_tickers):
    # receives the dictionary of lists of symbols.  Attempts to insert them
    # into the database.
    # if the symbol already exists, then it will generate an exception that is
    # ignored.
    # this procedure ensures that the database always contains the current set
    # of
    # symbols in the S&P 500.  Note that any symbols that fall out of the S&P
    # 500 list
    # will remain in the database table and will not be removed.
    # iterate through the list of symbols in this sector

    # Old code to insert / update the table in the MSSQL database.  This code has been replaced
    # with code to put the data in Azure Table Storage:
    ###with pypyodbc.connect('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') as connection:
    ###    cursor = connection.cursor()
    ###    for sector, symbols in sector_tickers.items():
    ###        for symbol in symbols:
    ###            param_list = [symbol, sector]
                
    ###            try:
    ###                cursor.execute("insert into Symbols(Symbol,Sector) values (?,?)",param_list)
    ###                lg.error('New options symbol added for ' + symbol)
    ###            except Exception:
    ###                pass
    ###    connection.commit()

    # Code to update the Azure Table Storage:

    account = ut.get_azure_account()   # CloudStorageAccount(is_emulated=True)
    table_service = None
    table_name = ut.TABLE_NAME_SYMBOLS

    try:
        table_service = account.create_table_service()

        if not table_service.exists(table_name):
            # create the table
            try:
                table_service.create_table(table_name)
            except Exception as err:
                print('Error creating table, ' + table_name + 'check if it already exists')
                lg.error('Tried and failed to create the table for the symbols.  Program terminating...')


        for sector, symbols in sector_tickers.items():
            for symbol in symbols:
                try:
                    symbol_entity = {'PartitionKey': 'Symbol', 'RowKey': symbol, 'sector' : sector }
                    table_service.insert_entity(table_name,symbol_entity)
                    lg.error('New options symbol added for ' + symbol)
                except Exception as e:
                    #lg.error('Error inserting into symbols table for ' + symbol)
                    pass

        print('Azure Storage Table Symbols - Completed.')
    except Exception as e:
        print('Error occurred in the sample. If you are using the emulator, please make sure the emulator is running.', e)



def insert_options(optionChain):
    # receives the optionChain object containing all options for all expiration dates
    # for the selected symbol.  inserts rows into the database options table for
    # each option.  Performs a db INSERT statement.  If the row already exists,
    # the database will generate an invalid key error to prevent the row from
    # being duplicated in the table.  In this case, the error is ignored.
    #
    # changes to use an entity instead of a parameter list so we can cast the date objects for dates in Azure
    ###with pypyodbc.connect('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') as connection:
    ###    cursor = connection.cursor()
    ###    stockPrice = optionChain.stockPrice
    ###    for o in optionChain.options:
            ### original code:
            ###param_list = [o.symbol, o.optionDate, o.expiration, o.callPut, o.strike, o.bid, o.ask, o.lastPrice, o.volume, o.openInterest, o.impliedVolatility, stockPrice]
            ###try:
            ###    updateString = ('insert into Options (Symbol, OptionDate, Expiration, CallPut, Strike, Bid, Ask, LastPrice, Volume, OpenInterest, IV, StockPrice)'
            ###               ' values (?,?,?,?,?,?,?,?,?,?,?,?)')
            ###    cursor.execute(updateString, param_list)
            ###except Exception as err:
            ###    lg.error('Insert failed for symbol ' + o.symbol + ' exp: ' + o.expiration)
        ###connection.commit()
            ###
            ### replacement code to use entity objects and Azure table storage:
    account = ut.get_azure_account()   # CloudStorageAccount(is_emulated=True)
    table_service = None
    try:
        table_service = account.create_table_service()

        if not table_service.exists(ut.TABLE_NAME_OPTIONS):
            # create the table
            try:
                table_service.create_table(ut.TABLE_NAME_OPTIONS)
            except Exception as err:
                print('Error creating table, ' + ut.TABLE_NAME_OPTIONS + 'check if it already exists')                
                lg.error('Tried and failed to create the table for the symbols.  Program terminating...')
                exit()

        batch = TableBatch()
        batchCount = 0
        rowCount = 0
        print('Number entries to handle is ' + str(len(optionChain.options)))
        for o in optionChain.options:
            rowCount += 1
            if rowCount > 100:
                # Azure restricts the batch size to a max of a hundred entries.  Since we're at our
                # limit, we'll commit these and start a new batch
                table_service.commit_batch(ut.TABLE_NAME_OPTIONS, batch)
                batch = TableBatch()
                rowCount = 1
                batchCount += 1

            option = Entity()
            option.PartitionKey = o.symbol
            # rowkey comprises the concatination of symbols to ensure the key is unique for the symbol.
            # we'll use the callPut, optionDate, expirationDate, and strike price.  Dates will be in format yyyymmdd
            option.RowKey = o.callPut + o.optionDate.strftime('%Y%m%d') + o.expiration.strftime('%Y%m%d') + str(o.strike)
            option.OptionDate = ut.date_for_azure(o.optionDate)
            option.Expiration = ut.date_for_azure(o.expiration)
            option.CallPut = o.callPut
            option.Strike = o.strike
            option.Bid = o.bid
            option.Ask = o.ask
            option.LastPrice = o.lastPrice
            option.Volume = o.volume
            option.OpenInterest = o.openInterest
            option.IV = o.impliedVolatility
            option.StockPrice = o.stockPrice

            batch.insert_entity(option)

        table_service.commit_batch(ut.TABLE_NAME_OPTIONS, batch)


    except Exception as e:
        print('Error occurred in the sample. If you are using the emulator, please make sure the emulator is running.', e)
        lg.error('Error adding rows to the options table')


def update_stock(stock):
    # receives the stock object containing the day's performance for the stock symbol.
    # adds the stock row to the Azure Table Storage for the stocks entity.

    account = ut.get_azure_account()   # CloudStorageAccount(is_emulated=True)
    table_service = None
    try:
        table_service = account.create_table_service()

        if not table_service.exists(ut.TABLE_NAME_STOCKS):
            # create the table
            try:
                table_service.create_table(ut.TABLE_NAME_STOCKS)
            except Exception as err:
                print('Error creating table, ' + ut.TABLE_NAME_STOCKS + 'check if it already exists')

        try:
            strDate = stock.date.strftime('%Y%m%d')
            stock_entity = {'PartitionKey': stock.symbol, 'RowKey': strDate, 'open' : stock.open, 'high' : stock.high, 'low' : stock.low, 'close' : stock.close, 'volume' : stock.volume }
            table_service.insert_entity(ut.TABLE_NAME_STOCKS,stock_entity)
        except Exception as e:
            lg.error('Error inserting into stock table for ' + symbol)

    except Exception as e:
        pass


def insert_historical_option_data(rows):
    account = ut.get_azure_account()   
    table_service = None
    table_name = 'optionSandbox' # ut.TABLE_NAME_OPTIONS
    try:
        if config.IS_EMULATED:
            table_service = TableService(is_emulated = True)
        else:
            table_service = TableService(
                account_name = config.STORAGE_ACCOUNT_NAME,
                account_key = config.STORAGE_ACCOUNT_KEY
                )

        if not table_service.exists(table_name):
            # create the table
            try:
                table_service.create_table(table_name)
            except Exception as err:
                print('Error creating table, ' + table_name + 'check if it already exists')                
                lg.error('Tried and failed to create the table for the symbols.  Program terminating...')
                exit()

        batch = TableBatch()
        batchCount = 0
        rowCount = 0

        for row in rows:
            option = Entity()
            callPut = str(row[5]).strip()
            optionDate = row[7]
            expiration = row[6]
            strike = float(row[8])
            option.PartitionKey = str(row[0]).strip()
            # rowkey comprises the concatination of symbols to ensure the key is unique for the symbol.
            #option.RowKey = callPut + optionDate.strftime('%Y%m%d') + expiration.strftime('%Y%m%d') + str(strike)
            optionDateYYYY = optionDate[-4:]
            optionDateMM = optionDate[:2]
            optionDateDD = optionDate[3:5]
            optionDateRowKey = optionDateYYYY + optionDateMM + optionDateDD
            expDateYYYY = expiration[-4:]
            expDateMM = expiration[:2]
            expDateDD = expiration[3:5]
            expDateRowKey = expDateYYYY + expDateMM + expDateDD

            option.RowKey = callPut + optionDateRowKey + expDateRowKey + str(strike)
            option.OptionDate = ut.historicalLoadDates[optionDate] #  ut.date_for_azure(optionDate)
            option.Expiration = ut.historicalLoadDates[expiration] #  ut.date_for_azure(expiration)
            option.CallPut = callPut
            option.Strike = strike
            option.Bid = float(row[10])
            option.Ask = float(row[11])
            option.LastPrice = float(row[9])
            option.Volume = float(row[12])
            option.OpenInterest = int(row[13])
            option.StockPrice = float(row[1])
            batch.insert_or_replace_entity(option)

        table_service.commit_batch(table_name, batch)

    except Exception as e:
        print('Error importing option ' + symbol + '. Error is: ', e)
        lg.error('Error importing rows to the options table')


def insert_options_azure(optionChain):
    # receives the optionChain object containing all options for all expiration dates
    # for the selected symbol.  inserts rows into the database options table for
    # each option.  Performs a db INSERT statement.  If the row already exists,
    # the database will generate an invalid key error to prevent the row from
    # being duplicated in the table.  In this case, the error is ignored.
    #
    account = ut.get_azure_account()   
    table_service = None
    table_name = ut.TABLE_NAME_OPTIONS
    try:
        if config.IS_EMULATED:
            table_service = TableService(is_emulated = True)
        else:
            table_service = TableService(
                account_name = config.STORAGE_ACCOUNT_NAME,
                account_key = config.STORAGE_ACCOUNT_KEY
                )

        if not table_service.exists(table_name):
            # create the table
            try:
                table_service.create_table(table_name)
            except Exception as err:
                print('Error creating table, ' + table_name + 'check if it already exists')                
                lg.error('Tried and failed to create the table for the symbols.  Program terminating...')
                exit()

        batch = TableBatch()
        batchCount = 0
        rowCount = 0
        for o in optionChain.options:
            rowCount += 1
            if rowCount > 100:
                # Azure restricts the batch size to a max of a hundred entries.  Since we're at our
                # limit, we'll commit these and start a new batch
                table_service.commit_batch(table_name, batch)
                batch = TableBatch()
                rowCount = 1
                batchCount += 1

            option = Entity()
            option.PartitionKey = o.PartitionKey
            # rowkey comprises the concatination of symbols to ensure the key is unique for the symbol.
            # we'll use the callPut, optionDate, expirationDate, and strike price.  Dates will be in format yyyymmdd
            option.RowKey = o.RowKey
            option.OptionDate = o.optionDate  # dates are already cast as Entity Property with an aware date value
            option.Expiration = o.expiration
            option.CallPut = o.callPut
            option.Strike = o.strike
            option.Bid = o.bid
            option.Ask = o.ask
            option.LastPrice = o.lastPrice
            option.Volume = o.volume
            option.OpenInterest = o.openInterest
            option.IV = o.impliedVolatility
            option.StockPrice = o.stockPrice

            batch.insert_entity(option)

        table_service.commit_batch(table_name, batch)


    except Exception as e:
        print('Error adding option ' + symbol + '. Error is: ', e)
        lg.error('Error adding rows to the options table')


    
'''
This method is used only for a unit test in development to ensure that we can
write to the mssql database from the azure webjobs account.
'''
def write_log_file_entry(symbol, currentDate, rowCount):
    # writes data to the log file in the MSSQL database logfile table
    with pypyodbc.connect('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') as connection:
        cursor = connection.cursor()
        param_list = [symbol, currentDate, rowCount]
        try:
            cursor.execute("insert into LogFile(Symbol, OptionDate, NumberRows) values (?,?,?)",param_list)
        except Exception as err:
            pass
        connection.commit()


def insert_test_entity():
    account = ut.get_azure_account()   # CloudStorageAccount(is_emulated=True)
    table_service = None
    try:
        table_service = account.create_table_service()

        if not table_service.exists(ut.TABLE_NAME_AZURETEST):
            # create the table
            try:
                table_service.create_table(ut.TABLE_NAME_AZURETEST)
            except Exception as err:
                print('Error creating table, ' + ut.TABLE_NAME_AZURETEST + 'check if it already exists')

        try:
            test_entity = {'PartitionKey': 'rwr', 'RowKey': 'abc', 'field1' : 'value1' }
            table_service.insert_entity(ut.TABLE_NAME_AZURETEST,test_entity)
        except Exception as e:
            lg.error('Error inserting into test_entity table')

    except Exception as e:
        pass