#!/usr/bin/env python
# coding: utf-8
import time
import configparser

# Couchbase initialization
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

# Read configurations
config = configparser.ConfigParser()
config.read('globalconfig.ini')

couchb_url = config['APP_CONFIG']['COUCHBASE_URL']
couchb_user = config['APP_CONFIG']['COUCHBASE_USERNAME']
couchb_pwd = config['APP_CONFIG']['COUCHBASE_PASSWORD']
couchb_bucket = config['APP_CONFIG']['COUCHBASE_BUCKET']
type_status = config['APP_CONFIG']['TYPE_STATUS']
status_initiated = config['APP_CONFIG']['STATUS_INITIATED']
v2v_algoname_cmd = config['APP_CONFIG']['V2V_ALGONAME_CMD']
b2b_algoname_cmd = config['APP_CONFIG']['B2B_ALGONAME_CMD']
v2b_algoname_cmd = config['APP_CONFIG']['V2B_ALGONAME_CMD']
mostpop_algoname_cmd = config['APP_CONFIG']['MOSTPOP_ALGONAME_CMD']
v2v_algoname = config['APP_CONFIG']['V2V_ALGONAME']
b2b_algoname = config['APP_CONFIG']['B2B_ALGONAME']
v2b_algoname = config['APP_CONFIG']['V2B_ALGONAME']
mostpop_algoname = config['APP_CONFIG']['MOSTPOP_ALGONAME']
na_value = config['APP_CONFIG']['NA_VALUE']

cluster = Cluster(couchb_url)
authenticator = PasswordAuthenticator(couchb_user, couchb_pwd)
cluster.authenticate(authenticator)
cb = cluster.open_bucket(couchb_bucket)

fobj = open('instance.txt', 'r')

for l in fobj:
	print(l)
	larr = l.split()
	print(larr[1])
	
	if(larr[1] == mostpop_algoname_cmd):
		paramlist = larr[2].split('###')[0]
		paramlist = paramlist.replace("[[", "").replace("]]", "").replace("'", "")
		paramlist = [x.split(',') for x in paramlist.split('],[')]

		for sublist in paramlist:
			categoryval = str(sublist[0])
			popmeasure = str(sublist[1])
			orgid = str(sublist[2])
			timeframe = str(sublist[3])
			channeltenantid = str(sublist[4])
			catalogid = str(sublist[5])
			zoneids = str(sublist[6:]).replace("[", "").replace("]", "").replace("'", "").replace(" ", "").split(",")

			for zoneid in zoneids:
				data = {}
				data['algorithmName'] = mostpop_algoname
				data['orgId'] = orgid
				data['timeFrame'] = timeframe
				data['channelTenantId'] = channeltenantid
				data['catalogId'] = catalogid
				data['category'] = categoryval
				data['measureOfPopularity'] = popmeasure
				data['zoneId'] = zoneid
				launchtime = l.split('###')[1] # launchtime is same for all algos in instance.txt
				launchtimeCB = launchtime.replace('\n', "").replace('_', ' ').replace('#', ':')
				data['launchTime'] = launchtimeCB
				data['status'] = status_initiated
				data['type'] = type_status
				print('Data to Couchbase: ' + str(data))
				docid = data['algorithmName'] + "##" + data['orgId'] + "##" + data['channelTenantId'] + "##" + data['catalogId'] + "##" + \
				data['zoneId'] + '##' + data['timeFrame'] + "##" + data['launchTime'] + "##" + data['category'] + "##" + data['measureOfPopularity']
				cb.upsert(docid, data)

	elif (larr[1] == v2b_algoname_cmd or larr[1] == v2v_algoname_cmd or larr[1] == b2b_algoname_cmd):
		data = {}
		if (larr[1] == v2b_algoname_cmd):
			data['algorithmName'] = v2b_algoname
		elif (larr[1] == v2v_algoname_cmd):
			data['algorithmName'] = v2v_algoname
		elif (larr[1] == b2b_algoname_cmd):
			data['algorithmName'] = b2b_algoname
		
		data['orgId'] = larr[2]
		data['timeFrame'] = larr[3]
		data['channelTenantId'] = larr[4]
		data['catalogId'] = larr[5]
		data['category'] = na_value
		data['measureOfPopularity'] = na_value
		zoneids = larr[6: len(larr)-1]
		data['launchTime'] = larr[-1].replace('_', ' ').replace('#', ':')
		data['status'] = status_initiated
		data['type'] = type_status

		for zoneid in zoneids:
			data['zoneId'] = zoneid
			docid = data['algorithmName'] + "##" + data['orgId'] + "##" + data['channelTenantId'] + "##" + data['catalogId'] + "##" + \
			data['zoneId'] + '##' + data['timeFrame'] + "##" + data['launchTime'] + "##" + data['category'] + "##" + data['measureOfPopularity']
			print('Data to Couchbase: ' + str(data))
			cb.upsert(docid, data)
