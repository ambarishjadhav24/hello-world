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
import sys
import time
from get_yaml_configs import getYamlConfigs,getCurrentTime
  
# Function to dump execution time for each phase in couchbase

def dumpToCouchbase (orgId, channelTenantId, timeframe, category, popmeasure, couchbObj, algorithmName, logMessages, totalTime,
                     preProcessing, writeToKafka, instanceExecution, launchtimeCB, naValue, yaml_file, timeType):
                      
    try:        
        dataDump = {}      
        clusterData = getYamlConfigs(yaml_file, logMessages)
        dataDump["NodeCount"] = clusterData['NodeCount']
        dataDump["NodeMemory"] = clusterData['NodeMemory']
        dataDump["NodeCores"] = clusterData['NodeCores']
        
        
        dataDump["OrgId"] = orgId
        dataDump["ChannelTenantId"] = channelTenantId
        dataDump["AlgoName"] = algorithmName
        dataDump['TimeFrame'] = timeframe
        dataDump['UserCount'] = naValue
        dataDump['ItemCount'] = naValue    
        dataDump["MeasureOfPop"] = popmeasure
        dataDump["Category"] = category
        dataDump['launchTime'] = launchtimeCB
        dataDump['type'] = timeType
        
        for element in [preProcessing, instanceExecution, writeToKafka, totalTime]:
            dataDump["AlgoPhase"] = str(element["AlgoPhase"])
            dataDump["StartTime"] = str(element["StartTime"])
            dataDump["EndTime"] = str(element["EndTime"])
            totaltime = element['EndTime'] - element['StartTime']
            totaltime = totaltime.total_seconds()
            dataDump["TotalTime"] = str(totaltime)
            logMessages.append(getCurrentTime() + " INFO " + "Dumping  the timelog to couchbase.." + str(dataDump["AlgoPhase"]))
            print("Dumping  the timelog to couchbase for phase " + str(dataDump["AlgoPhase"]))
            couchbObj.saveDataToCouchbase(dataDump)
            time.sleep(1)
            
        logMessages.append(getCurrentTime() + " INFO " + "Execution time dumped to couchbase successfully")
        print("Execution time dumped to couchbase successfully")
            
    except Exception as e:
        logMessages.append(getCurrentTime() + " ERROR " + str(sys.exc_info()))
        raise e
