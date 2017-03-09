import datetime
import time
import utility as ut


'''
This module creates and makes available as a reference a dictionary
that maps the string value of a date to an "aware" date object in python.
We need "aware" dates to insert into Azurr Table Storage so it will
recognize them as date types.  The code for converting from string to 
date and then from date to aware date is expensive.  We can improve on 
the performance by creating the hash map of dates one time and then
referencing it with our date values for insert statememts.

There are two versions of dates we need to convert.  The daily option
harvest gets the dates in an integer format that represents the number
of seconds since the epoch (1/1/1970).  This hash table converts
those values to a set of aware date objects.

A different module converts dates that are in the format that appears
in the historical options data download.
'''

class dateHashEpoch():

    def convertDateToSeconds(self, dateToConvert, epoch_date):
        # receives a date
        # returns the number of seconds since the epoch
        currentDateNoTime = str(datetime.datetime(dateToConvert.year, dateToConvert.month, dateToConvert.day))
        a = datetime.datetime.strptime(currentDateNoTime, "%Y-%m-%d %H:%M:%S")
        seconds = int((a-epoch_date).total_seconds())
        return seconds

    def createEpochDateHashMap(self):
        epoch_date = datetime.datetime(1970,1,1)
        currentDateTime = datetime.datetime.today()

        if len(ut.epochDates)==0:
            # build the hashmap for dates.  Use a date range that starts with
            # today (we won't need any past dates for our purposes of inserting
            # option data) and extend for a period of three years.  Most of the
            # future dates we'll be considering will be for option expiration dates,
            # and most of those are within the next few months, with the occasional
            # expiration date that goes out a year or so.  We'll use three years 
            # to be safe.

            year = datetime.datetime.today().year
            month = datetime.datetime.today().month
            day = datetime.datetime.today().day
            dateToConvert = datetime.datetime(year, month, day)
            delta = datetime.timedelta(days=1)

            for i in range(0, 365 * 3): # later change to 365 * 3 to get three years
                seconds = self.convertDateToSeconds(dateToConvert, epoch_date)
                date_for_azure = ut.date_for_azure(dateToConvert) # gets an entity property of the "aware" date for our dictionary

                # put the date and seconds-since-epoch on the dictionary
                ut.epochDates[str(seconds)] = date_for_azure

                # for development only, let's print them out so we can check them for accuracy:
                #print(str(dateToConvert) + ' ' + str(seconds))
                dateToConvert += delta

    def createHistoricalLoadDateHashMap(self):
        # create a hash map of dates with the key in the same format as the date fields
        # in the import of the historical options data.  The value in this key / value
        # relationship is that the key matches the date value from the import, while
        # the associated value is an entity property that wraps an aware date value.
        # our hashmap key value date range should be from the date of the first option
        # in our historical date, which is Jan 1, 2016 through the date of the
        # latest date in our historical data.  This is probably about three years out
        # from the last option date of December 31, 2016.  So maybe four years of dates.
    
        if len(ut.historicalLoadDates)==0:
            date = datetime.datetime(2015,1,1)
            for i in range(0, 365 * 5): 
                # format the date to string format that matches format in historical data import
                dateKey = date.strftime('%m/%d/%Y')
                date_for_azure = ut.date_for_azure(date) # gets an entity property of the "aware" date for our dictionary
                # put the date and seconds-since-epoch on the dictionary
                ut.historicalLoadDates[dateKey] = date_for_azure
                date += datetime.timedelta(days=1)



