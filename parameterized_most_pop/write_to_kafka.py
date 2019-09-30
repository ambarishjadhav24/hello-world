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

from kafka import KafkaProducer
from kafka import KafkaClient
from kafka import SimpleProducer
from datetime import datetime
import sys
import json
from get_yaml_configs import getCurrentTime

# Function to save the raw recommendations in Kafka

def dumpToKafka (kafkaServer, kafkaTopic, popmeasurecompute, algorithmName, category, channelTenantId, popmeasure,
                 orgId, timeframe, zoneIds, logMessages, popmargin, poprevenue, popViewCount, popBuyCount, naValue,
                 kafkaViewCount, kafkaBuyCount):

    # Format the recs in the format expected by Kafka(json format)

    # Changing the dictionary values for writing to Kafka
    popmeasureDict = {popViewCount : kafkaViewCount, popBuyCount : kafkaBuyCount, poprevenue : poprevenue, popmargin : popmargin}

    recs_string = {}
    recs_string['targetProducts'] = naValue
    recs_string['mlRecommendation'] = popmeasurecompute
    recs_string['algorithmName'] = algorithmName
    recs_string['category'] = category
    recs_string['channelTennentId'] = channelTenantId
    recs_string['measureofPop'] = popmeasureDict[popmeasure]
    recs_string['modelRunDate'] = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    recs_string['orgId'] = orgId
    recs_string['timeframe'] = timeframe
    recs_string['zones'] =  zoneIds
    recs_string = json.dumps(recs_string)
    recs_string = str.encode(recs_string)
    
    logMessages.append(getCurrentTime() + ' INFO '+ 'Writing raw-recommendations to Kafka with following details:')
    logMessages.append(getCurrentTime() + ' INFO '+ 'Kafka Server(s): ' + kafkaServer)
    logMessages.append(getCurrentTime() + ' INFO '+ 'Kafka Topic: ' + kafkaTopic)

    try:
        logMessages.append(getCurrentTime() + ' INFO ' + 'Writing recommendations to Kafka')

        # Create Kafka connection
        client = KafkaClient(kafkaServer)
        producer = SimpleProducer(client)

        # Send recommendations to Kafka
        producer.send_messages(kafkaTopic,recs_string)
        client.close()
        print('Recommendations written to Kafka successfully')
        logMessages.append(getCurrentTime() + ' INFO ' + 'Recommendations written to Kafka successfully')
        
    except Exception as e:
        logMessages.append(getCurrentTime() + " ERROR " + str(sys.exc_info()))
        raise e
