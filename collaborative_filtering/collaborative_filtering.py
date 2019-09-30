'''
***************************************************************
*
* Copyright © 2019 Acoustic, L.P. All rights reserved.
*
* NOTICE: This file contains material that is confidential and proprietary to
* Acoustic, L.P. and/or other developers. No license is granted under any intellectual or
* industrial property rights of Acoustic, L.P. except as may be provided in an agreement with
* Acoustic, L.P. Any unauthorized copying or distribution of content from this file is
* prohibited.
*
****************************************************************
'''

import configparser
import pandas as pd
import sys
import datetime
import numpy as np
import gensim
import json
import time
import boto3
from datetime import timedelta
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity
from get_yaml_configs import getCurrentTime, getYamlConfigs
from filter_events_data import filterEventsData
from save_time_log import saveData
from write_to_kafka import writeToKafka

logMessages = []
zoneIDs = []
docIds = []
dataUpdate = {}
def main():
	try:
		s3 = 'null'
		# Flag to check job status
		flag = 0
		
		# StartTime counter starts here
		startTime = datetime.datetime.now()
		PreProcessing = {}
		PreProcessing["StartTime"] = startTime
		
		algoName = sys.argv[1].split()[0]
		argLength = len(sys.argv[1].split())
		launchtime = sys.argv[1].split()[argLength - 1]
		print("LaunchTime: " + str(launchtime))
		launchtimeCB = launchtime.replace('_', ' ').replace('#', ':')
		timeFrame = sys.argv[1].split()[2]
		logFileName = launchtime + "_" + algoName + '_' + timeFrame +'.log'
		logMessages.append(getCurrentTime() + " INFO " + 'Algorithm for execution ' + algoName)
		logMessages.append(getCurrentTime() + " INFO " + 'Reading the global config file')
		
		# Read configurations
		config = configparser.ConfigParser()
		config.read('globalconfig.ini')
		no_of_recs_to_serve = config['APP_CONFIG']['NUMBER_OF_RECS']
		kafka_server = config['APP_CONFIG']['KAFKA_SERVER']
		kafka_topic = config['APP_CONFIG']['KAFKA_TOPIC']
		v2v_algoname = config['APP_CONFIG']['V2V_ALGONAME']
		b2b_algoname = config['APP_CONFIG']['B2B_ALGONAME']
		v2v_algoname_cmd = config['APP_CONFIG']['V2V_ALGONAME_CMD']
		b2b_algoname_cmd = config['APP_CONFIG']['B2B_ALGONAME_CMD']
		status_success = config['APP_CONFIG']['STATUS_SUCCESS']
		status_failure = config['APP_CONFIG']['STATUS_FAILURE']
		status_running = config['APP_CONFIG']['STATUS_RUNNING']
		timeframe_one = config['APP_CONFIG']['TIMEFRAME_ONE']
		timeframe_seven = config['APP_CONFIG']['TIMEFRAME_SEVEN']
		timeframe_thirty = config['APP_CONFIG']['TIMEFRAME_THIRTY']
		na_value = config['APP_CONFIG']['NA_VALUE']
		yaml_file = config['APP_CONFIG']['YAML_FILE']
		type_status = config['APP_CONFIG']['TYPE_STATUS']
		type_time = config['APP_CONFIG']['TYPE_TIME']
		timestamp_column =  config['INPUT_DATA_CONFIG']["TIMESTAMP_COL"]
		channel_tenant_column = config["INPUT_DATA_CONFIG"]["CHANNEL_TENANT_COL"]
		org_id_column = config["INPUT_DATA_CONFIG"]["ORG_COL"]
		event_column = config['INPUT_DATA_CONFIG']['EVENT_COL']
		groupByColumn = config['VIEW_TO_VIEW_OR_BUY_TO_BUY_CONFIG']['GROUP_BY_COLUMN']
		pivotColumn = config['VIEW_TO_VIEW_OR_BUY_TO_BUY_CONFIG']['PIVOT_COLUMN']
		activity_limit_for_remove = config['VIEW_TO_VIEW_OR_BUY_TO_BUY_CONFIG']['ACTIVITY_LIMIT_TO_REMOVE']
		no_of_features = config['VIEW_TO_VIEW_OR_BUY_TO_BUY_CONFIG']['NUMBER_OF_FEATURES']
		viewEvent = config["EVENT_CONFIG"]["VIEW"]
		buyEvent = config["EVENT_CONFIG"]["BUY"]
		
		
		# Reading aws and s3 configs
		aws_access_key = config['APP_CONFIG']["AWS_ACCESS_KEY"]
		aws_secret_key = config['APP_CONFIG']["AWS_SECRET_KEY"]
		s3_input_csv = config['APP_CONFIG']["S3_INPUT_CSV"]
		s3_logs_path = config['APP_CONFIG']["S3_LOGS_PATH"]
		s3_events_file = config['APP_CONFIG']["S3_EVENTS_FILE"]
		s3_endpoint_url = config['APP_CONFIG']["S3_ENDPOINT_URL"]
		s3_session_client = config['APP_CONFIG']["S3_SESSION_CLIENT"]
		s3_bucket = config['APP_CONFIG']["S3_BUCKET"]

		# Reading couchbase config
		couchb_url = config['APP_CONFIG']['COUCHBASE_URL']
		couchb_user = config['APP_CONFIG']['COUCHBASE_USERNAME']
		couchb_pwd = config['APP_CONFIG']['COUCHBASE_PASSWORD']
		couchb_bucket = config['APP_CONFIG']['COUCHBASE_BUCKET']
		print("Configuration reading done")
		logMessages.append(getCurrentTime() + " INFO " + "Configuration reading done")
				   
		# Reading the command line arguments
		org_ID = sys.argv[1].split()[1]
		channelTenant_ID = sys.argv[1].split()[3]
		catalog_ID = sys.argv[1].split()[4]
		zoneIDs = sys.argv[1].split()[5 : argLength - 1]
		logMessages.append(getCurrentTime() + " INFO " + "OrgID: " + org_ID + " ChannelTenantID: " + channelTenant_ID + " Catalog_ID: " + catalog_ID + " AlgorithmName: " + algoName + " TimeFrame: " + timeFrame + " zoneIDs: " + str(zoneIDs))
		print('Zones: ' + str(zoneIDs))
		print("Length of argv 1: " + str(argLength))
		print("Command line arguments reading done")
		
		print('Creating couchbase object')
		logMessages.append(getCurrentTime() + " INFO " + "Creating couchbase object")
		couchbObj = saveData(couchb_url, couchb_user, couchb_pwd, couchb_bucket)
		
		if algoName == v2v_algoname_cmd:
			eventName = viewEvent
			algoName = v2v_algoname
		elif algoName == b2b_algoname_cmd:
			eventName = buyEvent
			algoName = b2b_algoname
		else:
			logMessages.append(getCurrentTime() + " ERROR " + "The algoname should be either \"ViewToView\" or \"BuyToBuy\". Passed algo name is" + algoName)
			print("The algoname should be either \"ViewToView\" or \"BuyToBuy\". Passed algo name is " + algoName)
			raise Exception("The algoname should be either \"ViewToView\" or \"BuyToBuy\"")

		dataUpdate['algorithmName'] = algoName
		dataUpdate['orgId'] = org_ID
		dataUpdate['timeFrame'] = timeFrame
		dataUpdate['channelTenantId'] = channelTenant_ID
		dataUpdate['catalog_Id'] = catalog_ID
		dataUpdate['category'] = na_value
		dataUpdate['measureOfPopularity'] = na_value
		dataUpdate['launchTime'] = launchtimeCB
		dataUpdate['type'] = type_status
		dataUpdate['status'] = status_running
		dataUpdate['startTime'] = str(startTime)

		for zoneid in zoneIDs:
			docid = algoName + "##" + org_ID + "##" + channelTenant_ID + "##" + catalog_ID + '##' + zoneid + "##" + timeFrame + "##" + launchtimeCB + '##' + na_value + \
			'##' + na_value
			docIds.append(docid)
			dataUpdate['zoneId'] = zoneid
			couchbObj.cb.upsert(docid, dataUpdate)
		
		# Create AWS S3 client to read events file and store log file
		try:
			session = boto3.Session(
				aws_access_key_id = aws_access_key,
				aws_secret_access_key = aws_secret_key,
			)
			s3 = session.client(s3_session_client, endpoint_url = s3_endpoint_url)
			logMessages.append(getCurrentTime() + " INFO " + 'Created aws-s3 client for endpoint: ' + str(s3_endpoint_url))
		except:
			message = getCurrentTime() + " ERROR " + "Could not create aws-s3 client for endpoint: " + str(s3_endpoint_url) + " aws-access-key: " + str(aws_access_key) + " aws_secret_key: " + str(aws_secret_key)
			print(message + str(sys.exc_info()))
			raise Exception("Could not create aws-s3 client for endpoint: " + str(s3_endpoint_url))
		
		try:
			# Downloading the csv file for specific org
			s3_input_csv = s3_input_csv + "/" + org_ID + "/" + s3_events_file
			logMessages.append(getCurrentTime() + " INFO " + 'Reading events csv file at ' + s3_input_csv + ' from bucket ' + s3_bucket + ' AWS Endpoint: ' + str(s3_endpoint_url))
			s3.download_file(s3_bucket, s3_input_csv, s3_events_file)
		except Exception:
			logMessages.append(getCurrentTime() + " ERROR " + 'Could not download file or File not found at AWS-S3 path: ' + s3_input_csv)
			raise Exception('Could not download file or File not found at path: ' + s3_input_csv)

		try:
			# Reading the downloaded csv file
			df = pd.read_csv(s3_events_file, header = None, names= [pivotColumn, event_column, timestamp_column, channel_tenant_column, org_id_column, groupByColumn],\
							dtype={pivotColumn: object, event_column: object, channel_tenant_column: object, org_id_column: object, groupByColumn: object})
		except Exception:
			logMessages.append(getCurrentTime() + " ERROR " + 'Could not read csv file' + s3_events_file)
			raise Exception('Could not read csv file: ' + s3_events_file)

		logMessages.append(getCurrentTime() + " INFO " + 'Successfully read events file in a dataframe')

		# Convert timestamp into readable date-time format
		print('Before timestamp conversion')
		print(df[timestamp_column].head())
		df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit = 's')

		df[timestamp_column] = df[timestamp_column].apply(lambda x: x.date())

		print('After timestamp conversion')
		print(df[timestamp_column].head())
		logMessages.append(getCurrentTime() + " INFO " + 'After timestamp conversion\n' + str(df.head()))
		
		df_filtered = filterEventsData(df, eventName, channelTenant_ID, org_ID, timestamp_column, event_column, channel_tenant_column, org_id_column, timeFrame, pivotColumn, activity_limit_for_remove, logMessages,
					      timeframe_one, timeframe_seven, timeframe_thirty)

		# Create pivot table
		pivot_df = pd.pivot_table(df_filtered, index = [groupByColumn], columns = [pivotColumn], values = [event_column], aggfunc = 'count')
		product_ids_positional_mapping = np.array(pivot_df.index)
		print("Pivot table created successfully")
		logMessages.append(getCurrentTime() + " INFO " + 'Pivot table created successfully')

		# Get pivot table in numpy array
		pivot_array = pivot_df.values
		pivot_array = np.nan_to_num(pivot_array)
		no_of_items = pivot_array.shape[0]
		no_of_users = pivot_array.shape[1]
		print("No of users are %d" %(no_of_users))
		print("No of items are %d" %(no_of_items))
		logMessages.append(getCurrentTime() + " INFO " + "No of users in Pivot: " + str(no_of_users))
		logMessages.append(getCurrentTime() + " INFO " + "No of items in Pivot: " + str(no_of_items))

		# Preprocessing time counter stops here
		PreProcessing["EndTime"] = datetime.datetime.now()
		PreProcessing["AlgoPhase"] = "PreProcessing"

		no_of_features = int(no_of_features)
		logMessages.append(getCurrentTime() + " INFO " + "Number of features from config: " + str(no_of_features))
		print("Number of features from config: " + str(no_of_features))
		# Handling a use case when #features >= #Users
		if (no_of_features >= no_of_users):
			no_of_features = no_of_users
			logMessages.append(getCurrentTime() + " WARN " + "Number of features greater than or equal to the number of columns/users. Number of features passed " + str(no_of_features) + " No. of columns in matrix " + str(no_of_users))
			print("Warning!! Number of features greater than the number of columns. Number of features passed ", no_of_features, "No. of columns in matrix", no_of_users)
			print("Considering number of features = ", str(no_of_features))
			logMessages.append(getCurrentTime() + " INFO " + "Considering number of features = " + str(no_of_features))

		# SVD calculation time counter starts here
		SVDCalculation = {}
		SVDCalculation["StartTime"] = datetime.datetime.now()

		# Calculate SVD after converting to sparce matrix
		logMessages.append(getCurrentTime() + ' INFO ' + 'Calculating SVD after converting to sparce matrix')
		sparse_pivot_array = sparse.csc_matrix(pivot_array)
		u, s = gensim.models.lsimodel.stochastic_svd(sparse_pivot_array, no_of_features, no_of_items)
		print("SVD calculations done successfuly")
		logMessages.append(getCurrentTime() + ' INFO ' + 'SVD calculations done successfuly')

		# SVD calculation time counter stops here
		SVDCalculation["EndTime"] = datetime.datetime.now()
		SVDCalculation["AlgoPhase"] = "SVDCalculation"

		# Matrixtransformation / postprocessing / similarity+mapping of indices to PIDs starts here
		MatrixTransformation = {}
		MatrixTransformation['StartTime'] = datetime.datetime.now()

		# Calculate cosine similarity
		logMessages.append(getCurrentTime() + ' INFO ' + 'Calculating Cosine similarity using item-feature matrix')
		similarity = cosine_similarity(u)
		logMessages.append(getCurrentTime() + ' INFO ' + 'Cosine similarity calculated successfully')

		# Function to get sorted list of recommendations for each product limited by number of recs
		def getRecsForItem(arrayRow):
			"""
			Sort the row in decending order by values and then put the arguments there
			"""
			sorted = np.flip(np.argsort(arrayRow))
			return sorted[0:int(no_of_recs_to_serve) + 1]
		
		if((int(no_of_recs_to_serve) > no_of_items) or (int(no_of_recs_to_serve) == no_of_items)):
			print('WARNING no_of_recs_to_serve are greater than no_of_items')
			logMessages.append(getCurrentTime() + ' WARN ' + 'No_of_recs_to_serve are greater than no_of_items.\n Setting the no_of_recs_to_serve = no_of_items - 1')
			no_of_recs_to_serve = no_of_items
		recs_indices = np.apply_along_axis(getRecsForItem, 1, similarity)

		# Add the first column as the product IDs to compensate the effect due to other products having similarity 1 to target product
		temp = np.zeros((recs_indices.shape[0], recs_indices.shape[1]), dtype = int)
		natural_indices = np.arange(0, recs_indices.shape[0])
		natural_indices = natural_indices.reshape(natural_indices.shape[0], 1)
		
		# Code to remove items appearing in recs
		recsList = list(recs_indices)
		naturalList = list(natural_indices)
		newRecsList = []
		for i in range(0, len(recsList)):
			t = list(recsList[i])
			if (naturalList[i] in t):
				t.remove(naturalList[i])
			else:
				t.remove(t[len(t) - 1])
			newRecsList.append(t)

		newRecsListArr = np.array(newRecsList)
		temp[:, 1:] = newRecsListArr
		temp[:, 0:1] = natural_indices
		recs_indices = temp

		# Function to map similarity matrix indices to original product indices
		def indicestoPIDs(indexArray):
			"""
			Convert the indices to product ids using the mapping aray created earlier.
			"""
			return product_ids_positional_mapping[indexArray]
		
		# Mapping the indices of the array to productIDs
		recs_PIDs = np.apply_along_axis(indicestoPIDs, 1, recs_indices)
		print("Recommendations in PID format are calculated")
		logMessages.append(getCurrentTime() + ' INFO ' + 'Recommendations generated are as follows:\n' + str(recs_PIDs))
		print('Recommendations generated are as follows:\n' + str(recs_PIDs))

		# Matrixtransformation / postprocessing / similarity+mapping of indices to PIDs stops here
		MatrixTransformation['EndTime'] = datetime.datetime.now()
		MatrixTransformation['AlgoPhase'] = 'MatrixTransformation'

		# WritetoKafka time counter starts here
		WriteToKafka = {} #
		WriteToKafka["StartTime"] = datetime.datetime.now()

		logMessages.append(getCurrentTime() + ' INFO ' + 'Writing raw-recommendations to Kafka with following details:')
		logMessages.append(getCurrentTime() + ' INFO ' + 'Kafka Server(s): ' + kafka_server)
		logMessages.append(getCurrentTime() + ' INFO ' + 'Kafka Topic: ' + kafka_topic)
		writeToKafka(kafka_server, kafka_topic, algoName, channelTenant_ID, org_ID, timeFrame, zoneIDs, recs_PIDs, logMessages, na_value)
		
		# WritetoKafka time counter stops here
		WriteToKafka["EndTime"] = datetime.datetime.now()
		WriteToKafka['AlgoPhase'] = "WriteToKafka"
		print("Data sent to Kafka")

		# Totaltime counter stops here
		TotalTime = {}
		TotalTime["StartTime"] = startTime # Calculated at the start of main()
		TotalTime["EndTime"] = datetime.datetime.now()
		TotalTime["AlgoPhase"] = "TotalTime"

		dumpData = {}
		dumpData['TimeFrame'] = timeFrame
		dumpData['UserCount'] = no_of_users
		dumpData['ItemCount'] = no_of_items
		dumpData["AlgoName"] = algoName
		dumpData["MeasureOfPop"] = na_value
		dumpData["Category"] = na_value
		dumpData["OrgId"] = org_ID
		dumpData["ChannelTenantId"] = channelTenant_ID
		dumpData["catalog_Id"] = catalog_ID
		dumpData["launchTime"] = launchtimeCB
		dumpData["type"] = type_time

		clusterData = getYamlConfigs(yaml_file, logMessages)
		dumpData["NodeCount"] = clusterData['NodeCount']
		dumpData["NodeMemory"] = clusterData['NodeMemory']
		dumpData["NodeCores"] = clusterData['NodeCores']
		
		logMessages.append(getCurrentTime() + ' INFO ' + 'Writing execution times/job status to Couchbase with below details:')
		logMessages.append(getCurrentTime() + ' INFO ' + 'Couchbase URL: ' + couchb_url)
		logMessages.append(getCurrentTime() + ' INFO ' + 'Couchbase Bucket: ' + couchb_bucket)
		for element in [PreProcessing, SVDCalculation, MatrixTransformation, WriteToKafka, TotalTime]:
			dumpData["AlgoPhase"] = element["AlgoPhase"]
			dumpData["StartTime"] = str(element["StartTime"])
			dumpData["EndTime"] = str(element["EndTime"])
			totaltime = element['EndTime'] - element['StartTime']
			totaltime = totaltime.total_seconds()
			dumpData["TotalTime"] = str(totaltime)
			logMessages.append(getCurrentTime() + " INFO " + "dumping  the timelog to couchbase for phase " + str(dumpData["AlgoPhase"]))
			print("Dumping  the timelog to couchbase for phase " + str(dumpData["AlgoPhase"]))
			couchbObj.saveDataToCouchbase(dumpData)
		
		# Setting flag to 1 to indicate successful job run
		flag = 1

	except Exception as e:
		logMessages.append(getCurrentTime() + " ERROR " + str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1]))
		print(" ERROR " + str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1]))
		raise e
	finally:
		print('In Finally block')
		if (s3 == 'null'):
			print('Some error occured before/while creating s3 client')
			for i in range(len(docIds)):
				dataUpdate['status'] = status_failure
				dataUpdate['zoneId'] = zoneIDs[i]
				couchbObj.cb.upsert(docIds[i], dataUpdate)
		else:
			listAsString = ''
			for l in logMessages:
				if (len(listAsString) == 0):
					listAsString = l
				else:
					listAsString += '\n' + l
			
			# AWS-S3 related code
			s3_logs_path = s3_logs_path + "/" + org_ID + "_" + channelTenant_ID + "/" + logFileName
			logMessages.append(getCurrentTime() + " INFO " + "In finally block")
			logMessages.append(getCurrentTime() + " INFO " + "Writing log messages to AWS-S3 " + s3_logs_path)
			Outfile = open(logFileName, "w")
			Outfile.write(listAsString)
			Outfile.close()
			s3.upload_file(logFileName, s3_bucket, s3_logs_path)
			
			if (flag == 1):
				try:
					print('Flag is 1, updating success')
					for i in range(len(docIds)):
						dataUpdate['status'] = status_success
						dataUpdate['zoneId'] = zoneIDs[i]
						couchbObj.cb.upsert(docIds[i], dataUpdate)
				except Exception as e:
					print('Exception appeared updating couchbase')
					raise Exception(e)
			else :
				try:
					print('Flag is 0, updating failure')
					for i in range(len(docIds)):
						dataUpdate['status'] = status_failure
						dataUpdate['zoneId'] = zoneIDs[i]
						couchbObj.cb.upsert(docIds[i], dataUpdate)
				except Exception as e:
					print('Exception appeared updating couchbase')
					raise Exception(e)

if __name__ == "__main__":
	logMessages.append(getCurrentTime() + " INFO " + "Calling main...... ")
	print("calling main.... ")
	main()
