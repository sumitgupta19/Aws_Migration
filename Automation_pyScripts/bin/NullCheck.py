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
	
	start_time=timestamp
	print('Starting time '+start_time)
	
	configurationFileName= config_file
	data = json.load(open(configurationFileName,'r'))

	j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,j_sourceQueryFileName,j_targetQueryFileName, j_tableListFileName, j_resultFileName, j_folderPath = [data.get(d1) for d1 in ['sql_Server','sql_Database','rs_Dbname','rs_Port','rs_User','rs_Password','rs_Host','sourceQueryFileName','targetQueryFileName','tableListFileName','resultFileName','folderPath']]

	tableListFileName = j_tableListFileName
	resultFileName = j_resultFileName+'_'+ start_time+'.csv'
	folderPath = j_folderPath
	sourceQueryFileName =j_sourceQueryFileName
	targetQueryFileName =j_targetQueryFileName
	#Output directory path
	if folderPath !="":
		if not os.path.exists(folderPath):
			os.makedirs(folderPath)
		filepath= os.path.join(folderPath, resultFileName)
	else:
		filepath= resultFileName
		
	return (j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,sourceQueryFileName,targetQueryFileName,tableListFileName,filepath,start_time)


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
		print ("Unable to connect to Red Shift")
		e = sys.exc_info()[1]
		print (e)
		
def nullCheck(sqlCursor,rsCursor,sourceQueryFileName,targetQueryFileName,tableListFileName,filepath,start_time):
	#Reading Table List from .csv file
	
	
	tableListFile = open(tableListFileName, 'r')
	tableCursor = csv.reader(tableListFile)
	next(tableCursor, None) # skip the headers
	tablelist=list(tableCursor)
	  
	fp = open(filepath, 'w+')
	fp.write('SchemaName'+','+'TableName'+','+'SourceNotNULLCount'+','+'TargetNotNULLCount'+"\n")
	
	sourceQueryFile = open(sourceQueryFileName, 'r')
	sourceQuery = sourceQueryFile.read()
	
	targetQueryFile = open(targetQueryFileName, 'r')
	targetQuery = targetQueryFile.read()

	for tablename in tablelist:     
	#SQl Server Query
	
		try:
			SQLCommand=(str(sourceQuery) % (str(tablename[0]),str(tablename[1])))
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
			redShiftCommand=(str(targetQuery) % (str(tablename[0]),str(tablename[1])))	
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
	sourceQueryFile.close()
	targetQueryFile.close()
	
	t = time.localtime()
	end_time = time.strftime('%Y%m%d%H%M', t)
	print('Process completed at time '+end_time)
	
	execution_time=int(end_time) -int(start_time)
	print('Process execution time '+ str(execution_time))

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()
	
    sqlserver, sqldatabase, rsDbname ,rsPort, rsUser, rsPassword, rsHost,sourceQueryFileName,targetQueryFileName,tableListFileName,filepath,start_time=getConfiguration(args.config_file)
    sql_con=sqlServer_connect(sqlserver,sqldatabase)
    rs_conn=redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost)
    nullCheck(sql_con,rs_conn,sourceQueryFileName,targetQueryFileName,tableListFileName,filepath,start_time)
    sql_con.close()
    rs_conn.close()

if __name__=='__main__':

    main()
