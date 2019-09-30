#################################################################################################
#                                                                                               #
# Copyright © 2019 Acoustic, L.P. All rights reserved.                                          #
#                                                                                               #
# NOTICE: This file contains material that is confidential and proprietary to                   #
# Acoustic, L.P. and/or other developers. No license is granted under any intellectual or       #
# industrial property rights of Acoustic, L.P. except as may be provided in an agreement with   #
# Acoustic, L.P. Any unauthorized copying or distribution of content from this file is          #
# prohibited.                                                                                   #
#                                                                                               #
#################################################################################################

from kafka import KafkaProducer
from kafka import KafkaClient
from kafka import SimpleProducer
from datetime import datetime
import sys
import json
from get_yaml_configs import getCurrentTime

# Format the recs in the format expected by Kafka
def formatToKafka(PIDArray, algoName, channel_tennentId, org_id, timeframe, ZoneIDs, na_value):
	"""
	convert to json format expected by Kafka
	"""
	recs_string = {}
	recs_string['targetProducts'] = PIDArray[0]
	recs_string['recommendations'] = PIDArray[1]
	recs_string['algorithmName'] = algoName
	recs_string['category'] = na_value
	recs_string['channelTennentId'] = channel_tennentId
	recs_string['measureofPop'] = na_value
	recs_string['modelRunDate'] = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
	recs_string['orgId'] = org_id
	recs_string['timeframe'] = timeframe
	recs_string['zones'] =  "#".join([zone for zone in ZoneIDs])
	recs_string = json.dumps(recs_string)
	return str.encode(recs_string)

def writeToKafka(kafka_server, kafka_topic, algoName, channelTenant_ID, org_ID, timeFrame, ZoneIDs, recs_PIDs, logMessages, na_value):

	try:
		logMessages.append(getCurrentTime() + ' INFO ' + 'Writing recommendations to Kafka')
		#create Kafka connection
		client = KafkaClient(kafka_server)
		producer = SimpleProducer(client)

		#send recommendations to Kafka for each product
		for row in recs_PIDs:
			producer.send_messages(kafka_topic, formatToKafka(row, algoName, channelTenant_ID, org_ID, timeFrame, ZoneIDs, 
									  na_value))
			print(formatToKafka(row, algoName, channelTenant_ID, org_ID, timeFrame, ZoneIDs, na_value))
		client.close()
		logMessages.append(getCurrentTime() + ' INFO '+'Recommendations written to Kafka successfully')
	except Exception as e:
		logMessages.append(getCurrentTime() + " ERROR " + str(sys.exc_info()))
		raise e
