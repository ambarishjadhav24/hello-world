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
from get_yaml_configs import getCurrentTime
import sys
import json

# Format the recs in the format expected by Kafka
def formatToKafka(pIDArray, algoName, channel_tennentId, org_id, timeframe, zoneIDs, na_value):
	"""
	Convert to json format expected by Kafka
	"""
	recs_string = {}
	target_product = pIDArray[0]
	recs_for_target_prod = pIDArray[1:]
	recs_string['targetProducts'] = target_product
	recs_string['recommendations'] = "#".join([product for product in recs_for_target_prod])
	recs_string['algorithmName'] = algoName
	recs_string['category'] = na_value
	recs_string['channelTennentId'] = channel_tennentId
	recs_string['measureofPop'] = na_value
	recs_string['modelRunDate'] = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
	recs_string['orgId'] = org_id
	recs_string['timeframe'] = timeframe
	recs_string['zones'] =  "#".join([zone for zone in zoneIDs])
	recs_string = json.dumps(recs_string)
	return str.encode(recs_string)

# Function to write data to Kafka
def writeToKafka(kafka_server, kafka_topic, algoName, channelTenant_ID, org_ID, timeFrame, zoneIDs, recs_PIDs, logMessages, na_value):

	try:
		logMessages.append(getCurrentTime() + ' INFO ' + 'Writing recommendations to Kafka')
		# Create Kafka connection
		client = KafkaClient(kafka_server)
		producer = SimpleProducer(client)

		# Send recommendations to Kafka for each product
		for row in recs_PIDs:
			producer.send_messages(kafka_topic, formatToKafka(row, algoName, channelTenant_ID, org_ID, timeFrame,
									  zoneIDs, na_value))
		client.close()
		logMessages.append(getCurrentTime() + ' INFO ' + 'Recommendations written to Kafka successfully')
		print("Recommendations written to Kafka successfully")
	except Exception as e:
		logMessages.append(getCurrentTime() + " ERROR " + str(sys.exc_info()))
		raise e
