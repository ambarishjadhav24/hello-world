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

from datetime import datetime
import requests
import json
import pandas as pd
import sys
from get_yaml_configs import getCurrentTime

# Function to pre process the events file, product catalog

def preProcessedData (productFeedUrl, logMessages, view, transaction, timestampColumn, visitorIdColumn,
                      itemIdColumn, eventColumn, orgIdColumn, channelTenantColumn, productIdColumn,
                      categoryColumn, marginColumn, priceColumn, channelTenantId, orgId, s3InputCsv,
                      s3EndpointUrl, s3Bucket, s3EventsFile, s3,catalogClientId, catalogClientSecret, catalogOfferingId, catalogAPIkey):

    # Read event/input file from S3 and throw error if file does not exists:

    try:    
        s3InputCsv = s3InputCsv + "/" + orgId + "/" + s3EventsFile
        print("s3InputCsv is:  " + str(s3InputCsv))            
        logMessages.append(getCurrentTime() + " INFO " + 'Reading events csv file at "' + s3InputCsv + '" from bucket "' + s3Bucket + '" AWS Endpoint: ' + str(s3EndpointUrl))
        s3.download_file(s3Bucket, s3InputCsv, s3EventsFile)
        
    except Exception:    
        logMessages.append(getCurrentTime() + " ERROR " + 'Could not download events file or File not found at AWS-S3 path: ' + s3InputCsv)
        raise Exception('Could not download file or File not found at path: ' + s3InputCsv)

    try:    
        events_file = pd.read_csv(s3EventsFile, header = None, names= [visitorIdColumn, eventColumn, timestampColumn, channelTenantColumn,orgIdColumn, itemIdColumn],\
                         dtype={visitorIdColumn: object, timestampColumn: object, eventColumn: object, channelTenantColumn: object, orgIdColumn: object, itemIdColumn: object})
        logMessages.append(getCurrentTime() + " INFO " + 'Successfully read events file in a dataframe')
        
    except Exception:    
        logMessages.append(getCurrentTime() + " ERROR " + 'Could not events read csv file' + s3EventsFile)
        raise Exception('Could not read csv file: ' + s3EventsFile)


    # Timestamp column data conversion to yyyy-MM-dd from unix format    
    logMessages.append(getCurrentTime() + " INFO " + 'Before timestamp conversion\n' + str(events_file.head()))

    # Code to check 10 or 13 digit timestamp    
    if(len(str(events_file[timestampColumn].max(axis=0))) == 10):
        events_file[timestampColumn] = pd.to_datetime(events_file[timestampColumn], unit='s')
        
    events_file[timestampColumn] = events_file[timestampColumn].apply(lambda x: x.date()) #Convert timestamp to datetime format
    logMessages.append(getCurrentTime() + " INFO " + 'After timestamp conversion\n' + str(events_file.head()))
    
    # Filter by orgId and channeltenantid    
    events_file = events_file[events_file[orgIdColumn] == orgId]
    events_file = events_file[events_file[channelTenantColumn] == channelTenantId]
    
    if(len(events_file.index) == 0):
        message = "Events data is not available for given orgId '" + orgId + "' or channelTenantId '" + channelTenantId + "'. Please verify"
        logMessages.append(getCurrentTime() + " ERROR " + message)
        raise Exception(getCurrentTime() + " ERROR " + message)
        
    else:
        message = 'Total number of entries in dataframe after org and channel filtering are %d : ' %(len(events_file))
        logMessages.append(getCurrentTime() + " INFO " + message)

    # Read product feed in memory and throw error if url is incorrect or service is down:    
    try:
        headerDict = {}
        headerDict['x-ibm-client-id'] = catalogClientId
        headerDict['x-ibm-client-secret'] = catalogClientSecret
        headerDict['x-ibm-dx-offering-id'] = catalogOfferingId
        headerDict['x-wps-api-key'] = catalogAPIkey
        catalogUrl = productFeedUrl + '/rtp-' + orgId + '/v1/instances/' + channelTenantId + '/documents?q=*:*&limit=10000000'
        requestData = requests.get(catalogUrl, headers = headerDict)
        catalogObj = requestData.json()
        data = catalogObj["documents"]
        product_feed = pd.io.json.json_normalize(data)
        message = "Product feed data found for orgId '" + orgId + "', channelTenantId '" + channelTenantId + "' at path: " + catalogUrl   
        logMessages.append(getCurrentTime() + " INFO " + message )
        
    except Exception:
        message = "Product feed data not found for orgId '" + orgId + "', channelTenantId '" + channelTenantId + "' at path: " + catalogUrl
        logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
        raise Exception(message)

    # Product feed file column names validation    
    mandateColumns = [productIdColumn,categoryColumn]
    
    for column in mandateColumns:
        try:
            product_feed[column]
            message = 'Column-name "' + column + '" found in product feed data for orgId:' + orgId + ', channelTenantId:' + channelTenantId
            logMessages.append(getCurrentTime() + " INFO " + message + "\n" + str(sys.exc_info()))
            
        except Exception:
            message = 'Column-name "' + column + '" not found in product feed data for orgId:' + orgId + ', channelTenantId:' + channelTenantId
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception (message)
   
    preprocessed_eventsfile = pd.merge(events_file, product_feed,left_on=itemIdColumn,right_on=productIdColumn,how='left')
    preprocessed_eventsfile = preprocessed_eventsfile.rename(columns={priceColumn:'price', marginColumn:'margin'})\
                                                     .drop([visitorIdColumn, productIdColumn],axis = 1)
                                                     
    return preprocessed_eventsfile
