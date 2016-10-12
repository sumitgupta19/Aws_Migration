#!/usr/bin/python

import pypyodbc
import psycopg2
import psycopg2.extras
import argparse
import sys
import csv
import json
import time, os, fnmatch, shutil
import pandas as pd

def getConfiguration(config_file):

	t = time.localtime()
	timestamp = time.strftime('%Y%m%d%H%M', t)
	tmpSysCol ="rowversion"
	#Reading all configuration from configuration.json file
	configurationFileName= config_file
	data = json.load(open(configurationFileName,'r'))
	j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,j_sourceFileName, j_targetFileName,j_tableListFileName, j_dataTypeFileName,j_resultFileName, j_folderPath = [data.get(d1) for d1 in ['sql_Server','sql_Database','rs_Dbname','rs_Port','rs_User','rs_Password','rs_Host','sourceFileName','targetFileName','tableListFileName','dataTypeFileName','resultFileName','folderPath']]
	sourceFileName = j_sourceFileName
	targetFileName = j_targetFileName
	tableListFileName = j_tableListFileName
	dataTypeFileName = j_dataTypeFileName
	resultFileName = j_resultFileName+'_'+ timestamp+'.csv'
	folderPath = j_folderPath

	#Output directory path
	if folderPath !="":
		if not os.path.exists(folderPath):
			os.makedirs(folderPath)
		filepath= os.path.join(folderPath, resultFileName)
	else:
		filepath= resultFileName
	return (j_sqlserver, j_sqldatabase, j_rsDbname ,j_rsPort, j_rsUser, j_rsPassword, j_rsHost,sourceFileName,targetFileName,tableListFileName,filepath,dataTypeFileName,timestamp,tmpSysCol)

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

def tableStructureCheck(sqlCursor,rsCursor,sourceFileName,targetFileName,tableListFileName,filepath,dataTypeFileName,timestamp,tmpSysCol):		
	#Reading Table List from .csv file
	tableListFile = open(tableListFileName, 'r')
	tableCursor = csv.reader(tableListFile)
	next(tableCursor, None)  # skip the headers

	sourcefile = open(sourceFileName, 'wb')
	sf = csv.writer(sourcefile, delimiter = ',', lineterminator='\n')
	targetfile = open(targetFileName, 'wb')
	tf = csv.writer(targetfile, delimiter = ',',lineterminator='\n')


	for tableDetail in tableCursor:
	#SQl Server Query   
		sqlCommand = ('select lower(table_Schema), lower(table_name),lower(column_name), lower(data_type), coalesce(character_maximum_length,numeric_precision) from information_schema.columns where lower(table_schema) =lower('+"'" + tableDetail[0] +"'"+ ') and lower(table_name)=lower('+"'"+ tableDetail[1]  +"'" +') order by 1,2,3,4')
		sqlCursor.execute(sqlCommand)
		sqlResult = sqlCursor.fetchall()
		#print sqlResult
		#Reading DataTypes from .csv file
		dataConversionFile = open(dataTypeFileName, 'r')
		dataConversionCursor = csv.reader(dataConversionFile)
		next(dataConversionCursor, None)  # skip the headers
			
		#Datatype Conversion logic    
		for dataType_row in dataConversionCursor:
			for index in range(len(sqlResult)):
				if (sqlResult[index][3] == dataType_row[0]): 
					sqlResult[index]= list(sqlResult[index])
					sqlResult[index][3] = dataType_row[1]				
					if (dataType_row[2]=="y"):
						sqlResult[index][4] =str(sqlResult[index][4]).replace(str(sqlResult[index][4]),'**')
					if (sqlResult[index][2] == tmpSysCol):
						sqlResult[index][3] = str(sqlResult[index][3]).replace(str(sqlResult[index][3]),'**')
						sqlResult[index][4] = str(sqlResult[index][4]).replace(str(sqlResult[index][4]),'**')
				sqlResult[index]= tuple(sqlResult[index])							
						
		#Writing SQL Server data to source file
		sf.writerows(sqlResult)
		dataConversionFile.close()
		
		#Red Shift Query
		redShiftCommand = ('select lower(table_Schema), lower(table_name),lower(column_name), lower(data_type), coalesce(character_maximum_length,numeric_precision) from information_schema.columns where lower(table_schema) =lower('+"'" + tableDetail[0] +"'"+ ') and lower(table_name)=lower('+"'"+ tableDetail[1]  +"'" +') order by 1,2,3,4')
		rsCursor.execute(redShiftCommand)
		redShiftResult = rsCursor.fetchall()

		#Writing Red Shift data to target file
		dataConversionFile = open(dataTypeFileName, 'r')
		dataConversionCursor_targ = csv.reader(dataConversionFile)
		next(dataConversionCursor_targ, None)  # skip the headers
		
		for dataType_row_targ in dataConversionCursor_targ:
			
			for index in range(len(redShiftResult)):
				redShiftResult[index]= list(redShiftResult[index])
				if (redShiftResult[index][3]==dataType_row_targ[1] and dataType_row_targ[2]=="y"):
					redShiftResult[index][4] = str(redShiftResult[index][4]).replace(str(redShiftResult[index][4]),'**')
				if (redShiftResult[index][2] ==tmpSysCol):
					redShiftResult[index][3] =str(redShiftResult[index][3]).replace(str(redShiftResult[index][3]),'**')
					redShiftResult[index][4] =str(redShiftResult[index][4]).replace(str(redShiftResult[index][4]),'**')	
				redShiftResult[index]= tuple(redShiftResult[index])	
		tf.writerows(redShiftResult)
	dataConversionFile.close()
		
	tableListFile.close()        
	sourcefile.close()    
	targetfile.close()

	#Comparing Source Data with Target Data
	fileSource = open(sourceFileName, 'r')
	fileTarget = open(targetFileName, 'r')
	fileCompare = open(filepath, 'w')

	curSource = csv.reader(fileSource)
	curTarget = csv.reader(fileTarget)
	curCompare = csv.writer(fileCompare,delimiter = ',',lineterminator='\n')

	targetlist = list(curTarget)

	for source_row in curSource:
		row = 1
		found = False
		for target_row in targetlist:
			
			if source_row == target_row:
				found = True
				break
			if source_row[0] == target_row[0] and source_row[1] == target_row[1] and source_row[2] == target_row[2] and source_row[3] != target_row[3] and source_row[4] != target_row[4]:
				
				source_row.append('Source')
				curCompare.writerows([source_row])
				target_row.append('Target')
				curCompare.writerows([target_row])
				curCompare.writerow(["ERROR:Data Type and Data Size is not matching with Target"])
				break
			elif source_row[0] == target_row[0] and source_row[1] == target_row[1] and source_row[2] == target_row[2] and source_row[3] != target_row[3] and source_row[4] == target_row[4]:
				
				source_row.append('Source')
				curCompare.writerows([source_row])
				target_row.append('Target')
				curCompare.writerows([target_row])
				curCompare.writerow(["ERROR:Data Type is not matching with Target"])
				break
			elif source_row[0] == target_row[0] and source_row[1] == target_row[1] and source_row[2] == target_row[2] and source_row[3] == target_row[3] and source_row[4] != target_row[4]:
				
				source_row.append('Source')
				curCompare.writerows([source_row])
				target_row.append('Target')
				curCompare.writerows([target_row])
				curCompare.writerow(["ERROR:Data Size is not matching with Target"])
				break
		else:
			source_row.append('Source')
			curCompare.writerows([source_row])
			curCompare.writerow(["ERROR:Not found in Target"])

			row = row + 1

	print("Source To Target Comparison Completed !!!")
	fileSource.close()
	fileTarget.close()
	fileCompare.close()

	#Comparing Target Data with Source Data
	fileSource = open(sourceFileName, 'r')
	fileTarget = open(targetFileName, 'r')
	fileCompare = open(filepath, 'a')

	curSource = csv.reader(fileSource)
	curTarget = csv.reader(fileTarget)
	curCompare = csv.writer(fileCompare,delimiter = ',',lineterminator='\n')

	sourcelist = list(curSource)

	for target_row in curTarget:
		row = 1
		found = False
		for source_row in sourcelist:
			
			if target_row == source_row:
				found = True
				break
			if target_row[0] == source_row[0] and target_row[1] == source_row[1] and target_row[2] == source_row[2] and target_row[3] != source_row[3] and target_row[4] != source_row[4]:
				
				target_row.append('Target')
				curCompare.writerows([target_row])
				source_row.append('Source')
				curCompare.writerows([source_row])
				curCompare.writerow(["ERROR:Data Type and Data Size is not matching with Source"])
				break
			elif target_row[0] == source_row[0] and target_row[1] == source_row[1] and target_row[2] == source_row[2] and target_row[3] != source_row[3] and target_row[4] == source_row[4]:
				
				target_row.append('Target')
				curCompare.writerows([target_row])
				source_row.append('Source')
				curCompare.writerows([source_row])
				curCompare.writerow(["ERROR:Data Type is not matching with Source"])
				break
			elif target_row[0] == source_row[0] and target_row[1] == source_row[1] and target_row[2] == source_row[2] and target_row[3] == source_row[3] and target_row[4] != source_row[4]:
				
				target_row.append('Target')
				curCompare.writerows([target_row])
				source_row.append('Source')
				curCompare.writerows([source_row])
				curCompare.writerow(["ERROR:Data Size is not matching with Source"])
				break
		else:
			target_row.append('Target')
			curCompare.writerows([target_row])
			curCompare.writerow(["ERROR:Not found in Source"])

			row = row + 1
	print("Target to Source Process Completed !!!")

	fileSource.close()
	fileTarget.close()
	fileCompare.close()

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()
	
    sqlserver, sqldatabase, rsDbname ,rsPort, rsUser, rsPassword, rsHost,sourceFileName,targetFileName,tableListFileName,filepath,dataTypeFileName,timestamp,tmpSysCol=getConfiguration(args.config_file)
    sql_con=sqlServer_connect(sqlserver,sqldatabase)
    rs_conn=redShift_connect(rsDbname ,rsPort, rsUser, rsPassword, rsHost)
    tableStructureCheck(sql_con,rs_conn,sourceFileName,targetFileName,tableListFileName,filepath,dataTypeFileName,timestamp,tmpSysCol)
    sql_con.close()
    rs_conn.close()

if __name__=='__main__':

    main()
