#!/usr/bin/python

import pypyodbc
import psycopg2
import psycopg2.extras
import argparse
import sys
import csv
import json
import time, os, fnmatch, shutil

def getConfiguration(config_file):
    t = time.localtime()
    timestamp = time.strftime('%Y%m%d%H%M', t)
    print('Starting time '+timestamp)
    configurationFileName= config_file
    data = json.load(open(configurationFileName,'r'))

    j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost, j_tableListFileName, j_resultFileName, j_folderPath = [data.get(d1) for d1 in ['rs_Dbname','rs_Port','rs_User','rs_Password','rs_Host','tableListFileName','resultFileName','folderPath']]

    tableListFileName = j_tableListFileName
    resultFileName = j_resultFileName+'_'+ timestamp+'.csv'
    folderPath = j_folderPath

    #Output directory path
    if folderPath !="":
        if not os.path.exists(folderPath):
            os.makedirs(folderPath)
        filepath= os.path.join(folderPath, resultFileName)
    else:
        filepath= resultFileName

    return (j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,tableListFileName,filepath,timestamp)

#Establishing Red Shift Connection 
def redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost):
	try:
		redShiftConnection = psycopg2.connect( ' dbname= ' +rsDbname +' port= ' +rsPort +' user= ' +rsUser +' password= ' +rsPassword +' host= ' +rsHost );
		print("Red Shift Connection Established...")
		rsCursor = redShiftConnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
		return rsCursor
	except:
		print ("Unable to connect to Red Shift")
		e = sys.exc_info()[1]
		print (e)

def duplicateCheck(rsCursor,tableListFileName,filepath,timestamp):
    #Reading Table List from .csv file
    tableListFile = open(tableListFileName, 'r')
    tableCursor = csv.reader(tableListFile)
    next(tableCursor, None) # skip the headers
    tablelist=list(tableCursor)
      
    fp = open(filepath, 'w+')
    fp.write('SchemaName'+','+'TableName'+','+'DuplicateCount'+"\n")
    for tablename in tablelist:     
    #Red Shift Query
            try:
				redShiftCommand = (' select column_name from information_schema.columns where lower(table_schema) =lower('+"'" + tablename[0] +"'"+ ') and lower(table_name)=lower('+"'"+ tablename[1]  +"'" +') order by ordinal_position' )
				rsCursor.execute(redShiftCommand)
				columnname = rsCursor.fetchall()
				
				columnlist = ','.join(elem[0] for elem in columnname)
				
				if tablename[2] !="":
					DuplicateCheckQuery = (' Select ' + columnlist +' from '  + tablename[0] +'.'+ tablename[1]+'  where '+ tablename[2] +' group by ' + columnlist +' having count(*) >1 ')
				else:
					DuplicateCheckQuery = (' Select ' + columnlist +' from '  + tablename[0] +'.'+ tablename[1] +' group by ' + columnlist +' having count(*) >1 ')

				#print(DuplicateCheckQuery)
				rsCursor.execute(DuplicateCheckQuery)
				duplicateresult = rsCursor.fetchall()
				count=len(duplicateresult)
				fp.write(str(tablename[0]) + "," + str(tablename[1]) + "," + str(count)+"\n")
				rsflag=0
            except:
                    print ("Red Shift Error Message:")
                    e = sys.exc_info()[1]
                    redShiftResult=e
                    print(redShiftResult)
                    rsflag=1

    fp.close()
    print('Process completed at time '+timestamp)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()
	
    rsDbname ,rsPort, rsUser, rsPassword, rsHost,tableListFileName,filepath,timestamp=getConfiguration(args.config_file)
    rs_conn=redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost)
    duplicateCheck(rs_conn,tableListFileName,filepath,timestamp)
    rs_conn.close()

if __name__=='__main__':

    main()

