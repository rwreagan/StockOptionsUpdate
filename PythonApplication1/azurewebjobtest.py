import sys, os
import database2 as db
import datetime as dt



'''
This is a job to experiment with azure webjobs deployment.  The only purpose of this job
is to provide a mechanism to determine if we can deploy to Azure and have the job run 
on a schedule.  We'll determine the success by whether it can write to both
Azure Table Storage and to MSSQL database.

use this line to flush the print buffer so we can see the output form the print statement:
sys.stdout.flush()
'''

currentDate = dt.date.today()


db.retrieve_symbols()
sys.stdout.flush()


#raise Exception('here you go: ' + myFolder)

# write a record to MS SQL
#db.write_log_file_entry('rwr', currentDate, 11)

# now write a record to Azure table storage
#db.insert_test_entity()