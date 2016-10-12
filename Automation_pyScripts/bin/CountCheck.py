#!/usr/bin/python

import pypyodbc
import psycopg2
import psycopg2.extras
import sys
import csv
import json
import argparse
import time, os, fnmatch, shutil

def getConfiguration(config_file):
	t = time.localtime()
	timestamp = time.strftime('%Y%m%d%H%M', t)
	print('Starting time '+timestamp)
	configurationFileName= config_file
	data = json.load(open(configurationFileName,'r'))

	j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost, j_tableListFileName, j_resultFileName, j_folderPath = [data.get(d1) for d1 in ['sql_Server','sql_Database','rs_Dbname','rs_Port','rs_User','rs_Password','rs_Host','tableListFileName','resultFileName','folderPath']]

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
		
	return (j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,filepath,tableListFileName,timestamp)


#Establishing Sql Server Connection 
def sqlServer_connect(sqlserver,sqldatabase): 
	try:
		sqlConnection = pypyodbc.connect('DRIVER={SQL Server};SERVER=' +
			sqlserver + ';DATABASE=' + sqldatabase + ';' +
			'Trusted_Connection=yes')
		print("SQl Sever Connection Established...")
		sqlCursor = sqlConnection.cursor()
		return sqlCursor
	except:
		print ("Unable to connect to SQL server")
		e = sys.exc_info()[1]
		print (e)

#Establishing Red Shift Connection 
def redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost):
	try:
		redShiftConnection = psycopg2.connect( ' dbname= ' +rsDbname +' port= ' +rsPort +' user= ' +rsUser +' password= ' +rsPassword +' host= ' +rsHost );
		print("Red Shift Connection Established...")
		rsCursor = redShiftConnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
		return rsCursor
	except:
		print ("Unable to connect to Red Shift server")
		e = sys.exc_info()[1]
		print (e)
def countCheck(sqlCursor,rsCursor,filepath,tableListFileName,timestamp):
	#Reading Table List from .csv file
	tableListFile = open(tableListFileName, 'r')
	tableCursor = csv.reader(tableListFile)
	next(tableCursor, None) # skip the headers
	tablelist=list(tableCursor)
	  
	fp = open(filepath, 'w+')
	fp.write('SchemaName'+','+'TableName'+','+'SourceCount'+','+'TargetCount'+"\n")
	for tablename in tablelist:     
	#SQl Server Query
		try:
			if tablename[2] !="":
				SQLCommand = (' SELECT Count(*) from '+ tablename[0] +'.'+tablename[1]+'  where '+ tablename[2])
			else:
				SQLCommand = (' SELECT Count(*) from '+ tablename[0]+'.'+tablename[1])
			sqlCursor.execute(SQLCommand)
			sqlResult = sqlCursor.fetchall()
			sqlflag=0	
		except:
			print ("SQL Server Error Message:")
			e = sys.exc_info()[1]
			sqlResult=e
			print (sqlResult)
			sqlflag=1
	#Red Shift Query
		try:
			if tablename[2] !="":
				redShiftCommand = (' SELECT Count(*) from '+ tablename[0] +'.'+tablename[1]+' where '+ tablename[2])
			else:
				redShiftCommand = (' SELECT Count(*) from '+ tablename[0]+'.'+tablename[1])      
			rsCursor.execute(redShiftCommand)
			redShiftResult = rsCursor.fetchall()
			rsflag=0
		except:
			print ("Red Shift Error Message:")
			e = sys.exc_info()[1]
			redShiftResult=e
			print(redShiftResult)
			rsflag=1

		if sqlflag==1 and rsflag==1:
			fp.write(str(tablename[0]) + "," + str(tablename[1]) + "," + str(sqlResult[1]) + "," + str(redShiftResult[0]))
		elif sqlflag==0 and rsflag==0:
			fp.write(str(tablename[0]) + "," + str(tablename[1]) + "," + str(sqlResult[0][0]) + "," + str(redShiftResult[0][0])+"\n") 
		elif sqlflag==0 and rsflag==1:
			fp.write(str(tablename[0]) + "," + str(tablename[1]) + "," + str(sqlResult[0][0]) + "," + str(redShiftResult[0])) 
		elif sqlflag==1 and rsflag==0:
			fp.write(str(tablename[0]) + "," + str(tablename[1]) + "," + str(sqlResult[0]) + "," + str(redShiftResult[0][0])) 		

	fp.close()

	print('Process completed at time '+timestamp)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()
	
    sqlserver, sqldatabase, rsDbname ,rsPort, rsUser, rsPassword, rsHost,filepath,tableListFileName,timestamp=getConfiguration(args.config_file)
    sql_con=sqlServer_connect(sqlserver,sqldatabase)
    rs_conn=redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost)
    countCheck(sql_con,rs_conn,filepath,tableListFileName,timestamp)
    sql_con.close()
    rs_conn.close()

if __name__=='__main__':

    main()


