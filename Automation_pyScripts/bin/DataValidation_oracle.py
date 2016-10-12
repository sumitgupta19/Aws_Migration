#!/usr/bin/python

import pypyodbc
import psycopg2
import psycopg2.extras
import difflib
import ConfigParser
import argparse
import sys
import csv
import json
import time, os, fnmatch, shutil
import pandas as pd
import cx_Oracle
from datetime import datetime

def getConfiguration(config_file):
	t = time.localtime()
	timestamp = time.strftime('%Y%m%d%H%M', t)
	
	start_time=timestamp
	print('Starting time '+start_time)
	
	#Reading all configuration from configuration.json file
	configurationFileName= config_file
	data = json.load(open(configurationFileName,'r'))

	j_orcUsername, j_orcPassword, j_orcIP, j_orcServiceName, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,j_sourceFileName, j_targetFileName,j_queryFileListFileName,j_resultFileName, j_folderPath = [data.get(d1) for d1 in [ 'orc_Username', 'orc_Password', 'orc_IP', 'orc_ServiceName','rs_Dbname','rs_Port','rs_User','rs_Password','rs_Host','sourceFileName','targetFileName','queryListFileName','resultFileName','folderPath']]
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

	return (j_orcUsername, j_orcPassword, j_orcIP, j_orcServiceName , j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time)

#Establishing Oracle Connection 
def oracle_connect(orcUsername, orcPassword, orcIP, orcServiceName): 

	path=os.path.abspath("bi_aws_migration_info.txt")
	config=ConfigParser.ConfigParser()
	config.read(path)
	orc_Username = config.get("oracle_cred", "orc_Username")
	orc_Password = config.get("oracle_cred", "orc_Password")
	orc_IP = config.get("oracle_cred", "orc_IP")
	orc_ServiceName = config.get("oracle_cred", "orc_ServiceName")
	
	try:
		connectionString=orc_Username+'/'+orc_Password+'@'+orc_IP+'/'+orc_ServiceName
		oracleCon=cx_Oracle.connect(unicode(connectionString, 'utf-8'))
		oracleCursor = oracleCon.cursor()
		print("Oracle Connection Established...")
		return oracleCursor
	except:
		print ("Unable to connect to Oracle")
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


def tableData_Comparison(oracleCursor,rsCursor,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time):
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
		print(tblname)
		
		try:
			sourceQueryFile = open(queryFile[0], 'r')
			sourceQuery = sourceQueryFile.read()
			oracleCommand=(str(sourceQuery))
			oracleCommand="SELECT * FROM AR.HZ_PARTIES WHERE ROWNUM <= 10"
			oracleQuery=unicode(oracleCommand, 'utf-8')
			print(oracleQuery)
			oracleCursor.execute(oracleQuery)
			oracleResult = oracleCursor.fetchall()
			#print(oracleResult)
			
			targetQueryFile = open(queryFile[1], 'r')
			targetQuery = targetQueryFile.read()
			rsCommand=(str(targetQuery))
			rsCursor.execute(rsCommand)
			rsResult = rsCursor.fetchall()

			resultFile = filepath+'/'+tblname+'_'+start_time+'_'+ str(counter)+'.csv'
			fileCompare = open(resultFile, 'wb')
			curCompare = csv.writer(fileCompare,delimiter = ',',lineterminator='\n')

			rsResultList=list(rsResult)
			
			sourcedict = dict()		
			result=[]
			for source_row in oracleResult:
				sourcedict[source_row[0]] = tuple(source_row)
			print("sourcedict")
			
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
    orcUsername, orcPassword, orcIP, orcServiceName , rsDbname ,rsPort, rsUser, rsPassword, rsHost,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time=getConfiguration(args.config_file)
    oracle_con=oracle_connect(orcUsername, orcPassword, orcIP, orcServiceName)
    rs_conn=redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost)
    tableData_Comparison(oracle_con,rs_conn,filepath,sourceFileName,targetFileName,queryFileListFileName, resultFileName, folderPath, start_time)
    oracle_con.close()
    rs_conn.close()

	
if __name__=='__main__':

    main()


