#!/usr/bin/python

import pypyodbc
import psycopg2
import psycopg2.extras
import difflib
import argparse
import sys
import csv
import json
import time, os, fnmatch, shutil
from datetime import datetime
#import pandas as pd

def getConfiguration(config_file):
	t = time.localtime()
	timestamp = time.strftime('%Y%m%d%H%M', t)
	
	start_time=timestamp
	print('Starting time '+start_time)
	
	#Reading all configuration from configuration.json file
	configurationFileName= config_file
	data = json.load(open(configurationFileName,'r'))

	j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,j_sourceFileName, j_targetFileName,j_queryFileListFileName,j_resultFileName, j_folderPath = [data.get(d1) for d1 in ['sql_Server','sql_Database','rs_Dbname','rs_Port','rs_User','rs_Password','rs_Host','sourceFileName','targetFileName','queryListFileName','resultFileName','folderPath']]
	sourceFileName = j_sourceFileName
	targetFileName = j_targetFileName
	queryFileListFileName = j_queryFileListFileName
	resultFileName = j_resultFileName
	folderPath = j_folderPath

	#Output directory path
	if folderPath !="":
		if not os.path.exists(folderPath):
			os.makedirs(folderPath)
		filepath= folderPath
	else:
		filepath= ""

	return (j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time)

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


def tableData_Comparison(sqlCursor,rsCursor,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time):
	#Reading Table List from .csv file
	queryFileList = open(queryFileListFileName, 'r')
	queryFileCursor = csv.reader(queryFileList)
	data = list(queryFileCursor)
	row_count = len(data)

	queryFileList = open(queryFileListFileName, 'r')
	queryFileCursor = csv.reader(queryFileList)
	next(queryFileCursor, None)  # skip the headers
	counter=1
	for queryFile in queryFileCursor:
		
		queryFileName=queryFile[0]
		search="/"
		index=queryFileName.rindex(search)
		tblname=queryFileName[index+1:-4]
		flag=0
		 
		try:
			sourceQueryFile = open(queryFile[0], 'r')
			sourceQuery = sourceQueryFile.read()
			sqlCommand=(str(sourceQuery))
			sqlCursor.execute(sqlCommand)
			sqlResult = sqlCursor.fetchall()
			
			targetQueryFile = open(queryFile[1], 'r')
			targetQuery = targetQueryFile.read()
			sqlCommand1=(str(targetQuery))
			rsCursor.execute(sqlCommand1)
			rsResult = rsCursor.fetchall()
			
			result = []
			resultFile = filepath+'/'+tblname+'_'+start_time+'_'+ str(counter)+'.csv'
			fileCompare = open(resultFile, 'wb')
			curCompare = csv.writer(fileCompare,delimiter = ',',lineterminator='\n')

			sqlResultList=list(sqlResult)
			rsResultList=list(rsResult)
			
			sourcedict = dict()		
			for source_row in sqlResultList:
				sourcedict[source_row[0]] = tuple(source_row)
			
			targetdict = dict()
			for target_row in rsResultList:
				targetdict[target_row[0]] = tuple(target_row)

			for key in sourcedict.keys():
				if key in targetdict.keys():
					if (sourcedict[key]!=targetdict[key]):
						result.append(sourcedict[key])
						result.append(targetdict[key])
						result.append(["ERROR:Data is not matching with Target"])
				else:
					result.append(sourcedict[key])
					result.append(["ERROR:Not found in Target"])

			flag=1
			print ("Data comparison completed...!!!")
			print(datetime.now())
			for item in result:
				curCompare.writerows([item])
			print(datetime.now())
			print ("Data written to file completed...!!!")
		except:
			print ("Comparision Failed for iteration :" +str(counter))
			e = sys.exc_info()[1]
			print (e)
			flag=0
			
		counter=counter+1
		print('Iteration '+str(counter-1)+' Completed')
		if(flag==1):
			fileCompare.close()
			sourceQueryFile.close()
			targetQueryFile.close()
		
	t = time.localtime()
	end_time = time.strftime('%Y%m%d%H%M', t)
	print('Process completed at time '+end_time)
	
	execution_time= int(end_time) - int(start_time)
	print('Process execution time '+ str(execution_time))
	
	queryFileList.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()
    sqlserver, sqldatabase, rsDbname ,rsPort, rsUser, rsPassword, rsHost,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time=getConfiguration(args.config_file)
    sql_con=sqlServer_connect(sqlserver,sqldatabase)
    rs_conn=redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost)
    tableData_Comparison(sql_con,rs_conn,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time)
    sql_con.close()
    rs_conn.close()

	
if __name__=='__main__':

    main()


