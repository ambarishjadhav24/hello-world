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

import sys
import time
import configparser
from datetime import timedelta
import datetime
import pandas as pd
from io import StringIO
import boto3
from data_preprocessing import preProcessedData
from save_time_log import saveData
from get_yaml_configs import getYamlConfigs,getCurrentTime
from write_to_kafka import dumpToKafka
from write_to_couchbase import dumpToCouchbase

def main():
    
    s3 = 'null'
    
    # Reading command line arguments      
    try:              
        paramList = sys.argv[1].split()[1]
    except Exception:
        message = 'Required system arguments not found'
        logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
        raise Exception(message)
    
    # Logging variables call
    try:        
        startTime = datetime.datetime.now()
        totalTime = {}
        totalTime["StartTime"] = startTime
        totalTime["AlgoPhase"] = "TotalTime"


        preProcessing={}
        preProcessing["StartTime"] = startTime
        preProcessing["AlgoPhase"] = "PreProcessing"
        
        logMessages = []

        # Read in the central configurations(config.ini )
        try:
            config = configparser.ConfigParser()
            config.read('globalconfig.ini')
            message = "Global config file found"
            logMessages.append(getCurrentTime() + " INFO " + message)
            
        except Exception:
            message = 'Global config file read failed'
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message)
        
        # Read aws and s3 configs
        awsAccessKey = config['APP_CONFIG']["AWS_ACCESS_KEY"]
        awsSecretKey = config['APP_CONFIG']["AWS_SECRET_KEY"]
        s3InputCsv = config['APP_CONFIG']["S3_INPUT_CSV"]
        s3LogsPath = config['APP_CONFIG']["S3_LOGS_PATH"]
        s3EventsFile = config['APP_CONFIG']["S3_EVENTS_FILE"]
        s3EndpointUrl = config['APP_CONFIG']["S3_ENDPOINT_URL"]
        s3SessionClient = config['APP_CONFIG']["S3_SESSION_CLIENT"]
        s3Bucket = config['APP_CONFIG']["S3_BUCKET"]
        yaml_file = config['APP_CONFIG']['YAML_FILE']
        
        algorithmName = config['APP_CONFIG']["MOSTPOP_ALGONAME"]
        timeframeOne = config['APP_CONFIG']["TIMEFRAME_ONE"]
        timeframeSev = config['APP_CONFIG']["TIMEFRAME_SEVEN"]
        timeframeThirty = config['APP_CONFIG']["TIMEFRAME_THIRTY"]
        naValue = config['APP_CONFIG']["NA_VALUE"]
        
        # Job status config
        statusSuccess = config['APP_CONFIG']["STATUS_SUCCESS"]
        statusFailure = config['APP_CONFIG']["STATUS_FAILURE"]
        statusRunning = config['APP_CONFIG']["STATUS_RUNNING"]
        statusType = config['APP_CONFIG']["TYPE_STATUS"]
        timeType = config['APP_CONFIG']["TYPE_TIME"]

        # Read couchbase config
        couchbUrl = config['APP_CONFIG']['COUCHBASE_URL']
        couchbUser = config['APP_CONFIG']['COUCHBASE_USERNAME']
        couchbPwd = config['APP_CONFIG']['COUCHBASE_PASSWORD']
        couchbBucket = config['APP_CONFIG']['COUCHBASE_BUCKET']
        
        # Read kafka config
        kafkaServer = config['APP_CONFIG']['KAFKA_SERVER']
        kafkaTopic = config['APP_CONFIG']['KAFKA_TOPIC']

        # Read event/input file config
        timestampColumn = config['INPUT_DATA_CONFIG']["TIMESTAMP_COL"]
        visitorIdColumn = config['INPUT_DATA_CONFIG']["VISITOR_ID_COL"]
        eventColumn = config['INPUT_DATA_CONFIG']["EVENT_COL"]
        itemIdColumn = config['INPUT_DATA_CONFIG']["ITEM_ID_COL"]
        channelTenantColumn = config['INPUT_DATA_CONFIG']['CHANNEL_TENANT_COL']
        orgIdColumn = config['INPUT_DATA_CONFIG']['ORG_COL']

        # Read catalog config
        productIdColumn = config['CATALOG_CONFIG']["PRODUCT_ID_COL"]
        categoryColumn = config['CATALOG_CONFIG']["CATEGORY_COL"]
        priceColumn = config['CATALOG_CONFIG']["PRICE_COL"]
        marginColumn = config['CATALOG_CONFIG']["MARGIN_COL"]
        productFeedUrl = config['CATALOG_CONFIG']['PRODUCT_FEED_URL']
        catalogClientId = config['CATALOG_CONFIG']['CATALOG_CLIENT_ID']
        catalogClientSecret = config['CATALOG_CONFIG']['CATALOG_CLIENT_SECRET']
        catalogOfferingId = config['CATALOG_CONFIG']['CATALOG_OFFERING_ID']
        catalogAPIkey = config['CATALOG_CONFIG']['CATALOG_API_KEY']

        # Read event column config
        view = config['EVENT_CONFIG']["VIEW"]
        transaction = config['EVENT_CONFIG']["BUY"]

        # Read parameters config
        poprevenue = config['PARAMETERS_CONFIG']["POPULARITY_REVENUE"]
        popmargin = config['PARAMETERS_CONFIG']["POPULARITY_MARGIN"]
        popViewCount = config['PARAMETERS_CONFIG']["POPULARITY_VIEWCOUNT"]
        popBuyCount = config['PARAMETERS_CONFIG']["POPULARITY_BUYCOUNT"]
        categoryAll = config['PARAMETERS_CONFIG']["CATEGORY_ALL"]
        
        kafkaViewCount = config['PARAMETERS_CONFIG']["KAFKA_VIEWCOUNT"]
        kafkaBuyCount = config['PARAMETERS_CONFIG']["KAFKA_BUYCOUNT"]     
        
        # Read miscellaneous config
        noOfRecs = config['APP_CONFIG']['NUMBER_OF_RECS']
        
        # Couchbase initialization
        logMessages.append(getCurrentTime() + " INFO " + 'Python version ' + sys.version)
        couchbObj = saveData(couchbUrl, couchbUser, couchbPwd, couchbBucket) 
        
        # Create aws-s3 client for endpoint:
        try:            
            session = boto3.Session(
                aws_access_key_id = awsAccessKey,
                aws_secret_access_key = awsSecretKey,
                )
            s3 = session.client(s3SessionClient, endpoint_url = s3EndpointUrl)
            logMessages.append(getCurrentTime() +" INFO " + 'Created aws-s3 client for endpoint: ' + str(s3EndpointUrl))
            
        except:        
            message = getCurrentTime() + " ERROR " + "Could not create aws-s3 client for endpoint '" + str(s3EndpointUrl) + "' aws-access-key '"+ str(awsAccessKey) + " awsSecretKey: " + str(awsSecretKey)
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message + str(sys.exc_info()))
        
        # Unpack input parameter lists
        if(type(paramList) == str):
            paramList = paramList.replace("[[", "").replace("]]", "").replace("'", "").replace("###", " ")
            paramLength = len(paramList.split())
            launchtime = paramList.split()[paramLength-1]
            launchtimeCB = launchtime.replace('_',' ').replace('#',':')
            paramList = paramList.replace(launchtime, "").replace(" ", "")
            paramList = [x.split(',') for x in paramList.split('],[')]
            
            for sublist in paramList:
                category = str(sublist[0])
                popmeasure = str(sublist[1])
                orgId = str(sublist[2])
                timeframe = str(sublist[3])
                channelTenantId = str(sublist[4])
                zoneIds = str(sublist[5:]).replace("[","").replace("]","").replace("'","").replace(" ","").replace(",","#")
                message = "Input parameters are " + orgId + ", " + channelTenantId + ", " + zoneIds + ", " + timeframe + ", " + category + ", " + popmeasure
                logMessages.append(getCurrentTime() + " INFO " + message)
                
                # Update existing job status to success/failure
                docIds = []
                ZoneID = sublist[5:]
                for zoneid in ZoneID:
                    docid = algorithmName + "##" + orgId + "##" + channelTenantId + "##" + zoneid + "##" + timeframe + "##" + launchtimeCB + '##' + category + \
                    '##' + popmeasure
                    docIds.append(docid)
                statusUpdate = {}
                statusUpdate['algorithmName'] = algorithmName
                statusUpdate['orgId'] = orgId
                statusUpdate['timeFrame'] = timeframe
                statusUpdate['channelTenantId'] = channelTenantId
                statusUpdate['category'] = category
                statusUpdate['measureOfPopularity'] = popmeasure
                statusUpdate['launchTime'] = launchtimeCB
                statusUpdate['type'] = statusType
                statusUpdate['status'] = statusRunning
                statusUpdate['startTime'] = str(startTime)
                
                print('Docid to update: '+ docid)
                try:                    
                    preprocessed = preProcessedData (productFeedUrl, logMessages, view, transaction, timestampColumn, visitorIdColumn,
                                                     itemIdColumn, eventColumn, orgIdColumn, channelTenantColumn, productIdColumn,
                                                     categoryColumn, marginColumn, priceColumn, channelTenantId, orgId, s3InputCsv,
                                                     s3EndpointUrl, s3Bucket, s3EventsFile, s3,catalogClientId, catalogClientSecret,
                                                     catalogOfferingId, catalogAPIkey)
                    
                    preProcessing["EndTime"] = datetime.datetime.now()
                    
                    
                    instanceExecution = {}
                    instanceExecution["StartTime"] = datetime.datetime.now()
                    instanceExecution["AlgoPhase"] = "InstanceExecution"
                    
                    timeframecatcompute = timeframeCatImpl (preprocessed, timeframe, timestampColumn, logMessages, categoryColumn, category,
                                                            orgId, channelTenantId, timeframeOne, timeframeSev, timeframeThirty, categoryAll)
                    
                    popmeasurecompute = popmeasureImpl (timeframecatcompute, popmeasure, eventColumn, priceColumn, view, transaction, itemIdColumn,
                                                        orgId, channelTenantId, timeframe, category, zoneIds, logMessages, noOfRecs, marginColumn,
                                                        algorithmName, popmargin, poprevenue, popViewCount, popBuyCount)
                    
                    instanceExecution["EndTime"] = datetime.datetime.now()
                    print("Raw Recommendations of Parameterized Most Popular is " + "\n" + str(popmeasurecompute))                    
                    
                    writeToKafka = {}
                    writeToKafka["StartTime"]=datetime.datetime.now()
                    writeToKafka["AlgoPhase"]="WriteToKafka"
                    
                    dumpToKafka (kafkaServer, kafkaTopic, popmeasurecompute, algorithmName, category, channelTenantId, popmeasure,
                                 orgId, timeframe, zoneIds, logMessages, popmargin, poprevenue, popViewCount, popBuyCount, naValue,
                                 kafkaViewCount, kafkaBuyCount)
                    
                    writeToKafka["EndTime"]=datetime.datetime.now()
                    totalTime["EndTime"]=datetime.datetime.now()
                    
                    dumpToCouchbase (orgId, channelTenantId, timeframe, category, popmeasure, couchbObj, algorithmName, logMessages,
                                     totalTime, preProcessing, writeToKafka, instanceExecution, launchtimeCB, naValue, yaml_file, timeType)
                    
                    try:
                        print('Job execution passed, updating success')
                        for i in range(len(docIds)):
                            statusUpdate['status'] = statusSuccess
                            statusUpdate['zoneId'] = ZoneID[i]
                            statusUpdate['startTime'] = str(startTime)
                            couchbObj.cb.upsert(docIds[i], statusUpdate)
                            
                    except Exception as e:
                        print('Some exception appeared updating success in couchbase')
                        raise Exception(e)                    
                    
                    
                except Exception as e:
                    message = "This instance execution failed due to " + str(e)
                    logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
                    print(message)
                    
                    try:
                        print('Job execution failed, updating failure')
                        for i in range(len(docIds)):
                            statusUpdate['status'] = statusFailure
                            statusUpdate['zoneId'] = ZoneID[i]
                            statusUpdate['startTime'] = str(startTime)
                            couchbObj.cb.upsert(docIds[i], statusUpdate)
                            
                    except Exception as e:
                        print('Some exception appeared updating failure in couchbase')
                        raise Exception(e)
                        
                    continue
                    
    finally:
        if (s3 == 'null'):
            print('Some error occured before/while creating s3 client. No logs written to s3')
            for i in range(len(docIds)):
                statusUpdate['status'] = statusFailure
                statusUpdate['zoneId'] = ZoneID[i]
                statusUpdate['startTime'] = str(startTime)
                couchbObj.cb.upsert(docIds[i], statusUpdate)
        else:
            listAsString = ''
            for l in logMessages:
                if (len(listAsString) == 0):
                    listAsString = l
                else:
                    listAsString += '\n' + l 
                    
        # Creating a log file
        logFileName = launchtime + '_MostPop.log'
        s3LogsPath = s3LogsPath + "/" + logFileName
        logMessages.append(getCurrentTime() + " INFO " + "In finally block")
        logMessages.append(getCurrentTime() + " INFO " + "Writing log messages to AWS-S3 "+ s3LogsPath)
        outFile=open(logFileName, "w")
        outFile.write(listAsString)
        outFile.close()
        s3.upload_file(logFileName, s3Bucket, s3LogsPath)
        
def timeframeCatImpl (preprocessed_eventsfile, timeframe, timestampColumn, logMessages, categoryColumn, category, orgId, channelTenantId,
                      timeframeOne, timeframeSev, timeframeThirty, categoryAll):

    # 1. Timeframe parameter implementation  
    if (timeframe == timeframeOne) or (timeframe == timeframeSev) or (timeframe == timeframeThirty):
        endDate = preprocessed_eventsfile[timestampColumn].max()
        logMessages.append(getCurrentTime() + " INFO " + "End date in current data is " + str(endDate))
        startDate = endDate - timedelta(days = int(timeframe))
        #current_date = datetime.date(datetime.now())
        logMessages.append(getCurrentTime() + " INFO " + "Start date in current data is " + str(startDate))
        logMessages.append(getCurrentTime() + " INFO " + "Timeframe Window: " + str(startDate) +" "+ str(endDate))
    else:
        message = "Given timeframe '" + str(timeframe) + "' is invalid." + "Valid options are " + timeframeOne + " or " + timeframeSev + " or " + timeframeThirty
        logMessages.append(getCurrentTime() +" ERROR " + message)
        raise Exception(message)

    message = 'Before time filtering Min date is ' + str(preprocessed_eventsfile[timestampColumn].min()) + ' and max date is ' + str(preprocessed_eventsfile[timestampColumn].max())
    logMessages.append(getCurrentTime() +" INFO " + message)
    
    message = 'Total number of entries in dataframe before timeframe filtering are %d' %(len(preprocessed_eventsfile))
    logMessages.append(getCurrentTime() +" INFO " + message)
    
    df_paramfilter = preprocessed_eventsfile[preprocessed_eventsfile[timestampColumn] >= startDate]
    df_paramfilter = df_paramfilter[df_paramfilter[timestampColumn] < endDate]
    
    message = 'Total number of entries in dataframe after timeframe filtering are %d' %(len(df_paramfilter))
    logMessages.append(getCurrentTime() +" INFO " + message)
    
    if(len(df_paramfilter.index) == 0):
        message = "The dataframe is empty for timeframe from " + str(startDate) + " to " + str(endDate) + ". Please verify the parameters passed"
        logMessages.append(getCurrentTime() +" ERROR " + message)
        raise Exception(message)
        
    else:        
        message = 'After time filtering Min date is ' + str(df_paramfilter[timestampColumn].min()) + ' and max date is '+ str(df_paramfilter[timestampColumn].max())
        logMessages.append(getCurrentTime() + " INFO " + message)

    # 2. Category parameter implementation
    if category == categoryAll:
        if (len(df_paramfilter.index) > 0):
            message = "Dataframe df_paramfilter has " + str(len(df_paramfilter)) + " record(s) for category: " + categoryAll + ", orgId: " + orgId + ", channelTenantId: " + channelTenantId
            logMessages.append(getCurrentTime() + " INFO " + message)
            
    else:
        df_paramfilter = df_paramfilter[df_paramfilter[categoryColumn].astype(str).str.contains(category)]
        if (len(df_paramfilter.index) > 0):
            message = "dataframe df_paramfilter has " + str(len(df_paramfilter)) + " record(s) for category: " + category + ", orgId: " + orgId + ", channelTenantId: " + channelTenantId
            logMessages.append(getCurrentTime() + " INFO " + message)

        else:
            message = "The dataframe is empty for category: " + category + ", orgid: " + orgId + ", channeltenantid: " + channelTenantId
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message)
            
    return df_paramfilter

def popmeasureImpl (df_paramfilter, popmeasure, eventColumn, priceColumn, view, transaction, itemIdColumn,
                    orgId, channelTenantId, timeframe, category, zoneIds, logMessages, noOfRecs, marginColumn,
                    algorithmName, popmargin, poprevenue, popViewCount, popBuyCount):
    
    
    # 1. Revenue/margin parameter implementation
    if popmeasure == poprevenue or popmeasure == popmargin:
        popmeasureDict = {poprevenue : 'price', popmargin : 'margin'}  
        df_popularity_measure = df_paramfilter[df_paramfilter[eventColumn] == transaction]
        
        if (len(df_popularity_measure.index) > 0):
            message = "Dataframe df_popularity_measure has " + str(len(df_popularity_measure)) + " record(s) for measureofpop: " + popmeasure + ", orgId: " + orgId + ", channelTenantId: " + channelTenantId
            logMessages.append(getCurrentTime() + " INFO " + message)
            
            df_popularity_measure = df_popularity_measure.groupby(itemIdColumn)[popmeasureDict[popmeasure]].sum().reset_index()
            df_popularity_measure.columns.values[1] = "mostpopular"
            df_popularity_measure = df_popularity_measure.sort_values(by='mostpopular', ascending=False)

        else:
            message = "The dataframe is empty for measureofpop: " + popmeasure + ", orgId: " + orgId + ", channelTenantId: " + channelTenantId
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message)       


    # 2. Viewcount/Buycount parameter implementation
            
    elif popmeasure == popViewCount or popmeasure == popBuyCount:
        popmeasureDict = {popViewCount : view, popBuyCount:transaction}
        df_popularity_measure = df_paramfilter[df_paramfilter[eventColumn] == popmeasureDict[popmeasure]]
        
        if (len(df_popularity_measure.index) > 0):
            message = "Dataframe df_popularity_measure has " + str(len(df_popularity_measure)) + " record(s) for measureofpop: " + popmeasure + ", orgId: " + orgId + ", channelTenantId: " + channelTenantId
            logMessages.append(getCurrentTime() + " INFO " + message)
            df_popularity_measure['mostpopular'] = 1
            df_popularity_measure = df_popularity_measure.groupby(itemIdColumn)['mostpopular'].sum().reset_index()\
                                                         .sort_values(by='mostpopular', ascending=False)

        else:
            message = "The dataframe is empty for measureofpop: " + popmeasure + ", orgId: " + orgId + ", channelTenantId: " + channelTenantId
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message)


    else:
        message = "Given measureofpopularity '" + str(popmeasure) + "' is invalid." + " Valid options are Revenue/Margin/View count/Buy count"
        logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
        raise Exception(message)
        
    message = "generating recommendations of Parameterized Most Popular Products"
    logMessages.append(getCurrentTime() + " INFO " + message)   
    df_popularity_measure = df_popularity_measure[:int(noOfRecs)].drop(['mostpopular'],axis=1)
    df_popularity_measure = '#'.join(df_popularity_measure[itemIdColumn])
    
    return df_popularity_measure

if __name__ == "__main__":
    print("calling main.... ")
    main()
