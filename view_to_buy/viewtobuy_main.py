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
from io import StringIO
import pandas as pd
import sys
from datetime import timedelta
import datetime
import numpy as np
from prefixspan import PrefixSpan
from session_conversion import session_convert
from basket_preparation import BasketPrep
from v2b_recs_generation import V2BSeggregation
from write_to_kafka import writeToKafka
from save_time_log import saveData
from get_yaml_configs import getYamlConfigs, getCurrentTime
import time
import boto3
import re

logMessages = []
def main():

    try:
        s3 = 'null'
        # flag to check job status
        flag = 0

        # StartTime counter starts here
        startTime = datetime.datetime.now()
        totalTime = {}
        totalTime["startTime"] = startTime
        totalTime["algoPhase"] = "TotalTime"

        preProcessing = {}
        preProcessing["startTime"] = startTime
        preProcessing["algoPhase"] = "PreProcessing"

        # Read in the command line arguments
        algoName=sys.argv[1].split()[0]
        org_ID =  sys.argv[1].split()[1]
        timeFrame = sys.argv[1].split()[2]
        channelTenant_ID = sys.argv[1].split()[3]
        argLength = len(sys.argv[1].split())
        zoneIDs = sys.argv[1].split()[4:argLength-1]
        launchTime = sys.argv[1].split()[argLength-1]
        logFileName = launchTime + "_" + algoName + "_" + timeFrame + '.log' #launchTime require in yyyy-mm-dd_HH#mm#ss
        launchTime = launchTime.replace('_', ' ').replace('#', ':') #launchTime convert in yyyy-mm-dd HH:mm:ss format

        logMessages.append(getCurrentTime() + " INFO " + 'Python version ' + sys.version)
        logMessages.append(getCurrentTime() + " INFO " + 'Reading the global config file')

        # Read configurations
        config = configparser.ConfigParser()
        config.read('globalconfig.ini')
        kafkaServer = config['APP_CONFIG']['KAFKA_SERVER']
        kafkaTopic = config['APP_CONFIG']['KAFKA_TOPIC']
        topk=config["VIEW_TO_BUY_CONFIG"]["TOPK"]
        noOfRecs = config['APP_CONFIG']['NUMBER_OF_RECS']
        algoName_sep = config['APP_CONFIG']['V2B_ALGONAME']
        statusPass = config['APP_CONFIG']['STATUS_SUCCESS']
        statusFail = config['APP_CONFIG']['STATUS_FAILURE']
        statusRun = config['APP_CONFIG']['STATUS_RUNNING']
        timeframe1 = config['APP_CONFIG']['TIMEFRAME_ONE']
        timeframe7 = config['APP_CONFIG']['TIMEFRAME_SEVEN']
        timeframe30 = config['APP_CONFIG']['TIMEFRAME_THIRTY']
        na_value = config['APP_CONFIG']['NA_VALUE']
        yamlFile = config['APP_CONFIG']['YAML_FILE']
        typeStatus = config['APP_CONFIG']['TYPE_STATUS']
        typeTime = config['APP_CONFIG']['TYPE_TIME']

        logMessages.append(getCurrentTime() + " INFO " + "Read config file with below Input Parameters")
        logMessages.append("OrgID: " + org_ID + "\nChannelTenantID: " + channelTenant_ID + "\nAlgorithmName: " + algoName_sep + "\nTimeFrame: " + timeFrame + "\nZoneIDs: " + str(zoneIDs))

        # Dataset Schema Configurations
        useridCol = config['INPUT_DATA_CONFIG']['VISITOR_ID_COL']
        eventCol = config['INPUT_DATA_CONFIG']['EVENT_COL']
        itemidCol = config['INPUT_DATA_CONFIG']['ITEM_ID_COL']
        transactionidCol = config['INPUT_DATA_CONFIG']['TRANSACTION_ID_COL']
        timestampCol = config['INPUT_DATA_CONFIG']['TIMESTAMP_COL']
        chtntidCol = config['INPUT_DATA_CONFIG']['CHANNEL_TENANT_COL']
        orgidCol = config['INPUT_DATA_CONFIG']['ORG_COL']

        # Event Column Configurations
        buyEvent = config['EVENT_CONFIG']['BUY']
        viewEvent = config['EVENT_CONFIG']['VIEW']

        # Reading aws and s3 configs
        awsAccessKey = config['APP_CONFIG']["AWS_ACCESS_KEY"]
        awsSecretKey = config['APP_CONFIG']["AWS_SECRET_KEY"]
        s3InputCsv = config['APP_CONFIG']["S3_INPUT_CSV"]
        s3LogsPath = config['APP_CONFIG']["S3_LOGS_PATH"]
        s3EventsFile = config['APP_CONFIG']["S3_EVENTS_FILE"]
        s3EndpointUrl = config['APP_CONFIG']["S3_ENDPOINT_URL"]
        s3SessionClient = config['APP_CONFIG']["S3_SESSION_CLIENT"]
        s3Bucket = config['APP_CONFIG']["S3_BUCKET"]

        # Reading couchbase config
        couchbUrl = config['APP_CONFIG']['COUCHBASE_URL']
        couchbUser = config['APP_CONFIG']['COUCHBASE_USERNAME']
        couchbPwd = config['APP_CONFIG']['COUCHBASE_PASSWORD']
        couchbBucket = config['APP_CONFIG']['COUCHBASE_BUCKET']

        # Couchbase DB connection and send Job status as RUNNING
        try:
            couchbObj = saveData(couchbUrl, couchbUser, couchbPwd, couchbBucket)
            # Job status schema
            statusUpdate  = {}
            statusUpdate['algorithmName'] = algoName_sep
            statusUpdate['orgId'] = org_ID
            statusUpdate['timeFrame'] = timeFrame
            statusUpdate['channelTenantId'] = channelTenant_ID
            statusUpdate['category'] = na_value
            statusUpdate['measureOfPopularity'] = na_value
            statusUpdate['launchTime'] = launchTime
            statusUpdate['type'] = typeStatus
            statusUpdate['status'] = statusRun
            statusUpdate['startTime'] = str(startTime)
            for zoneid in zoneIDs:
                docid = algoName_sep  + "##" + org_ID + "##" + channelTenant_ID + "##" + zoneid + "##" + timeFrame + "##" + launchTime + '##' + na_value + '##' + na_value
                statusUpdate['zoneId'] = zoneid
                couchbObj.cb.upsert(docid, statusUpdate)
                time.sleep(1)        
        except:
            message = "Some error occured while updating status as RUNNUNG in couchbase."
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception (message)

        # S3 Endpoint creation
        try:
            s3session = boto3.Session(
                aws_access_key_id = awsAccessKey,
                aws_secret_access_key = awsSecretKey,
                )
            s3 = s3session.client(s3SessionClient, endpoint_url = s3EndpointUrl)
            logMessages.append(getCurrentTime() + " INFO " + 'Created aws-s3 client for endpoint: '+ str(s3EndpointUrl))
        except:
            message = "Could not create aws-s3 client for endpoint: " + str(s3EndpointUrl) + " aws-access-key: "+ str(awsAccessKey) + " awsSecretKey: " + str(awsSecretKey)
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message)

        # Events data downloading from S3
        try:
            s3InputCsv = s3InputCsv + "/" + org_ID + "/" + s3EventsFile
            s3.download_file(s3Bucket, s3InputCsv, s3EventsFile)
            message = 'Event data successfully downloaded from ' + s3InputCsv + ' from bucket ' + s3Bucket + ' AWS Endpoint: '+ s3EndpointUrl
            logMessages.append(getCurrentTime() + " INFO " + message)
        except Exception:
            message = 'Event Data Couldnt downloaded successfully or File not found at AWS-S3 path: ' + s3InputCsv + ' from bucket ' + s3Bucket + ' AWS Endpoint: '+ s3EndpointUrl
            logMessages.append(getCurrentTime() + " ERROR " + message)
            raise Exception('Could not download file or File not found at path: ' + s3InputCsv)

        # Reading Events data
        try:
            eventsFile = pd.read_csv(s3EventsFile, header = None, skip_blank_lines = True, skipinitialspace = True)
            logMessages.append(getCurrentTime() + " INFO " + 'Successfully read events file in a dataframe')
        except Exception:
            logMessages.append(getCurrentTime() + " ERROR " + 'Could not read csv file' + s3EventsFile)
            raise Exception('Could not read csv file: ' + s3EventsFile)

        # Column renaming
        try:
            eventsFile.columns = [useridCol, eventCol, timestampCol, chtntidCol, orgidCol, itemidCol]
            eventsFile = eventsFile.astype({useridCol: object, eventCol : object, timestampCol : object, chtntidCol : object, orgidCol : str, itemidCol : object})
            logMessages.append(getCurrentTime() + " INFO " + 'Successfully column-names renamed in a dataframe')
        except Exception:
            message = "Column renaming unsuccessful due to number of column mismatch."
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception (message)
        
        # Assuming timestamp in 13 digit converting it into date
        eventsFile[timestampCol] = pd.to_datetime(eventsFile[timestampCol], unit='s')
        
        # Filter Events data for specific Org and Channel-Tennant
        eventsFile = eventsFile[(eventsFile[chtntidCol] == channelTenant_ID) & (eventsFile[orgidCol] == org_ID)]  
        if not bool(eventsFile.empty):
            message = "Event data suceessfully read for OrgID: " + org_ID + " and ChannelTennantID: " + channelTenant_ID
            logMessages.append(getCurrentTime() + " INFO " + message)
            logMessages.append(getCurrentTime() + " INFO " + 'No of rows:' + str(len(eventsFile)))
        else:
            message = 'Event data is not available for OrgID: ' + org_ID + ' and ChannelTennantID: ' + channelTenant_ID
            logMessages.append(getCurrentTime() + " ERROR " + message)
            raise Exception(message)

        # Session Conversion
        logMessages.append(getCurrentTime() + " INFO " + "Session will be converted with timespan of a day.")

        sessionFormatedDf = session_convert(eventsFile, timestampCol, eventCol, viewEvent, buyEvent, logMessages, timeFrame, timeframe1, timeframe7, timeframe30)
        logMessages.append(getCurrentTime() + " INFO " + "Session Conversion Done.")
        logMessages.append(getCurrentTime() + " INFO " + 'No of events:' + str(len(sessionFormatedDf)))
        userCount = len(sessionFormatedDf[useridCol].unique().tolist())
        itemCount = len(sessionFormatedDf[itemidCol].unique().tolist())
        logMessages.append(getCurrentTime() + " INFO " + "User Count: " + str(userCount))
        logMessages.append(getCurrentTime() + " INFO " + "Item Count: " + str(itemCount))
        print(sessionFormatedDf.head())

        # Basket Preparation
        logMessages.append(getCurrentTime() + " INFO " + "RawData stacking with Session & User specific")
        baskets = BasketPrep(sessionFormatedDf,useridCol,eventCol,viewEvent,buyEvent,itemidCol)
        logMessages.append(getCurrentTime() + " INFO " + 'No of pairs in Baskets: ' + str(len(baskets)))
        
        preProcessing["endTime"] = datetime.datetime.now()

        # Run prefixspan algo
        prefixSpanTopK = {}
        prefixSpanTopK["startTime"] = datetime.datetime.now()
        prefixSpanTopK["algoPhase"] = "PrefixSpanTopK"
        transactions = baskets['products'].tolist()
        prefixSpanVar = PrefixSpan(transactions)
        topkptrns = prefixSpanVar.topk(int(topk))
        logMessages.append(getCurrentTime() + " INFO " + "Top " + str(topk) + " patterns are requested")
        freqPtrns = pd.DataFrame(topkptrns, columns=['support', 'sequence'])
        prefixSpanTopK["endTime"] = datetime.datetime.now()

        # Generate Final Recommendations for View to Buy
        postProcessing = {}
        postProcessing["startTime"] = datetime.datetime.now()
        postProcessing["algoPhase"] = "PostProcessing"
        logMessages.append(getCurrentTime() + " INFO " + "Generating Recommendations: ")

        recs = V2BSeggregation(freqPtrns, 'sequence', buyEvent, viewEvent)
        try:
            not bool(recs.empty)
            recs['buyItems'] = recs['buyItems'].apply(lambda x:x[:int(noOfRecs)])
            logMessages.append(getCurrentTime() + " INFO " + 'No of patterns: ' + str(len(recs)))
        except Exception:
            message = "No recommendation generated."
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception (message)
        minsupportmsg = "The minimum support observed here is %d"%(freqPtrns.support.min())
        maxsupportmsg = "The maximum support observed here is %d"%(freqPtrns.support.max())
        logMessages.append(getCurrentTime() + " INFO " + minsupportmsg)
        logMessages.append(getCurrentTime() + " INFO " + maxsupportmsg)
        
        postProcessing["endTime"] = datetime.datetime.now()

        # WritetoKafka time counter starts here
        WriteToKafka = {}
        WriteToKafka["startTime"] = datetime.datetime.now()

        # Call write to kafka function
        logMessages.append(getCurrentTime() + ' INFO ' + 'Writing raw-recommendations to Kafka with following details:')
        logMessages.append(getCurrentTime() + ' INFO ' + 'Kafka Server(s): ' + kafkaServer)
        logMessages.append(getCurrentTime() + ' INFO ' + 'Kafka Topic: ' + kafkaTopic)
        recs['viewItems'] = recs['viewItems'].apply(lambda x:"%s" %"#".join(x))
        recs['buyItems'] = recs['buyItems'].apply(lambda x:"%s" %"#".join(x))
        try:
            writeToKafka(kafkaServer, kafkaTopic, algoName_sep, channelTenant_ID, org_ID, timeFrame, zoneIDs, recs.values, logMessages,na_value)
        except Exception:
            message = 'Some error occured writing raw-recommendations to Kafka.'
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception (message)

        # WritetoKafka time counter stops here
        WriteToKafka["endTime"] = datetime.datetime.now()
        WriteToKafka['algoPhase'] = "WriteToKafka"

        # Totaltime counter stops here
        totalTime["endTime"] = datetime.datetime.now()

        # Dump execution time of each phase in couchbase
        dumpData = {}
        dumpData['TimeFrame'] = timeFrame
        dumpData['UserCount'] = userCount
        dumpData['ItemCount'] = itemCount
        dumpData["AlgoName"] = algoName_sep
        dumpData["MeasureOfPop"] = na_value
        dumpData["Category"] = na_value
        dumpData["OrgId"] = org_ID
        dumpData["ChannelTenantId"] = channelTenant_ID
        dumpData["type"] = typeTime

        # Hardware parameters are hardcoded here
        clusterData = getYamlConfigs(yamlFile, logMessages)
        dumpData["NodeCount"] = clusterData['NodeCount']
        dumpData["NodeMemory"] = clusterData['NodeMemory']
        dumpData["NodeCores"] = clusterData['NodeCores']
        dumpData["launchTime"] = launchTime

        logMessages.append(getCurrentTime() + ' INFO ' + 'Writing execution times/job status to Couchbase with below details:')
        logMessages.append(getCurrentTime() + ' INFO ' + 'Couchbase URL: ' + couchbUrl)
        logMessages.append(getCurrentTime() + ' INFO ' + 'Couchbase Bucket: ' + couchbBucket)
        for element in [preProcessing,prefixSpanTopK,postProcessing,WriteToKafka,totalTime]:
            dumpData["AlgoPhase"] = str(element["algoPhase"])
            dumpData["StartTime"] = str(element["startTime"])
            dumpData["EndTime"] = str(element["endTime"])
            totaltime = element['endTime'] - element['startTime']
            totaltime = totaltime.total_seconds()
            dumpData["TotalTime"] = str(totaltime)
            couchbObj.saveDataToCouchbase(dumpData)
            time.sleep(1)
            message = 'Execution time dumped to couchbase for algoPhase: ' + str(element["algoPhase"])
            logMessages.append(getCurrentTime() + " INFO " + message)

        # Setting flag to 1 to indicate successful job run
        flag = 1

    except Exception as e:
        logMessages.append(getCurrentTime() + " ERROR " + '\n' + str(sys.exc_info()))
        raise e

    finally:
        print('In Finally block')
        
        # Job status Validation
        if (flag == 1) & (s3 != 'null'):
            statusUpdate['status'] = statusPass
        else:
            statusUpdate['status'] = statusFail

        # Dump Job status for each zone to couchbase
        try:
            for zoneid in zoneIDs:
                docid = algoName_sep  + "##" + org_ID + "##" + channelTenant_ID + "##" + zoneid + "##" + timeFrame + "##" + launchTime + '##' + na_value + '##' + na_value
                print('Docid to update: ' + docid)
                statusUpdate['zoneId'] = zoneid
                couchbObj.cb.upsert(docid, statusUpdate)
                message = 'Launchtime: ' + str(launchTime) + ' | Zone: ' + str(zoneid) + ' | Job status: ' + str(statusUpdate['status'])
                logMessages.append(getCurrentTime() + " INFO " + message)
                print("zone: " + zoneid)
        except Exception:
            raise Exception ("Some exception appeared updating couchbase \n" + str(sys.exc_info()))

        # AWS-S3 related code
        s3LogsPath = s3LogsPath + "/" + org_ID + "_" + channelTenant_ID + "/" + logFileName
        logMessages.append(getCurrentTime() + " INFO " + "Writing log messages to AWS-S3 " + s3LogsPath)
        listAsString = '\n'.join(logMessages)
        print(listAsString)
        outFile=open(logFileName, "w")
        outFile.write(listAsString)
        outFile.close()
        s3.upload_file(logFileName, s3Bucket, s3LogsPath)

if __name__ == "__main__":
    logMessages.append(getCurrentTime() +" INFO " + "Calling main...... ")
    main()
