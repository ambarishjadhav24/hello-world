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

from datetime import timedelta
from get_yaml_configs import getCurrentTime
import pandas as pd

# Function to filter data by timeframe, org, channel, event and n-time users
def filterEventsData(df, event, channel_tenant_id, org_id, timestamp_column, event_column, channel_tenant_column, org_id_column, timeframe, pivotColumn, activity_limit_for_remove, logMessages,
		    timeframe_one, timeframe_seven, timeframe_thirty):
  
	# 1. Filter by TimeFrame
	# Get the start date and end date from the data
	if (timeframe == timeframe_one) or (timeframe == timeframe_seven) or (timeframe == timeframe_thirty):
		logMessages.append(getCurrentTime() + " INFO " + "The timeframe passed is " + timeframe)
		end_date = df[timestamp_column].max()
		logMessages.append(getCurrentTime() + " INFO " + "End date in current data is " + str(end_date))
		start_date = end_date - timedelta(days = int(timeframe))
		print('Timeframe is %s' %(timeframe))
		print(start_date, end_date)
		logMessages.append(getCurrentTime() + " INFO " + "TimeFrame Window: " + str(start_date) + " to " + str(end_date))
	else:
		logMessages.append(getCurrentTime() + " ERROR " + "The timeframe parameter can only be 1 or 7 or 30")
		raise Exception("The timeframe parameter can only be 1 or 7 or 30")

	logMessages.append(getCurrentTime() + " INFO " + 'Before time filtering Min date is ' + str(df[timestamp_column].min()) + ' and max date is ' + str(df[timestamp_column].max()))
	df = df[df[timestamp_column] >= start_date]
	df = df[df[timestamp_column] < end_date]
	if(len(df.index) == 0):
		logMessages.append(getCurrentTime() + " ERROR " + "The dataframe is empty with given timeframe" + str(start_date) + " to " + str(end_date) + ". Please verify the parameters passed")
		raise Exception("The dataframe is empty with given timeframe" + str(start_date)+ " to " + str(end_date) + ". Please verify the parameters passed")
	else:
		print('After time filtering Min date is ' + str(df[timestamp_column].min()) + ' and max date is ' + str(df[timestamp_column].max()))
		logMessages.append(getCurrentTime() + " INFO " + 'After time filtering Min date is ' + str(df[timestamp_column].min()) + ' and max date is ' + str(df[timestamp_column].max()))
	print('Unique dates after filtering: ' + str(df[timestamp_column].unique()))
	
	# 2. Filter by orgID and channeltenantid
	df = df[df[org_id_column] == org_id]
	df = df[df[channel_tenant_column] == channel_tenant_id]
	if(len(df.index) == 0):
		logMessages.append(getCurrentTime() + " ERROR " + "The dataframe is empty with given Org_ID " + org_id + " and Channel_ID " + channel_tenant_id + ". Please verify the parameters passed")
		raise Exception(getCurrentTime() + " ERROR " + "The dataframe is empty with given Org_ID " + org_id + " and Channel_ID " + channel_tenant_id + ". Please verify the parameters passed")
	else:
		print('Total number of entries in dataframe after org and channel filtering are %d' %(len(df)))
		logMessages.append(getCurrentTime() + " INFO " + 'Total number of entries in dataframe after org and channel filtering are ' + str(len(df)))

	# 3. Filter by event (view/buy)
	print('Total number of entries in dataframe before event type filtering are %d' %(len(df)))
	logMessages.append(getCurrentTime() + " INFO " + 'Total number of entries in dataframe before event type filtering are %d' + str(len(df)))
	print('Unique Events: ' + str(df[event_column].unique()))
	df = df[df[event_column] == event]
	if(len(df.index) == 0):
		logMessages.append(getCurrentTime() + " ERROR " + "The dataframe is empty with given Event type " + event + ". Please verify the parameters passed")
		raise Exception(getCurrentTime() + " ERROR " + "The dataframe is empty with given Event type " + event + ". Please verify the parameters passed")
	else:
		print('Total number of entries in dataframe after event type filtering are %d' %(len(df)))
		logMessages.append(getCurrentTime() + " INFO " + "Total number of entries in dataframe after event type filtering are " + str(len(df)))

	# 4. Filter n-time users, n is one currently
	#get the userIDs and counts for those userIDs
	usersIds = df.groupby(df[pivotColumn]).count().index.to_series()
	counts = df.groupby(df[pivotColumn]).count()[event_column]
	eventCountsDF = pd.concat([counts, usersIds], axis=1) #create a Dataframe out of it

	eventCountsDF = eventCountsDF[eventCountsDF[event_column] > int(activity_limit_for_remove)]
	print('Events before removing %s time users are %d' %(activity_limit_for_remove, len(df)))
	logMessages.append(getCurrentTime() + " INFO " + 'Events before removing ' + activity_limit_for_remove + ' time users are ' + str(len(df)))
	df = df[df[pivotColumn].isin(eventCountsDF[pivotColumn])]
	if(len(df.index) == 0):
		logMessages.append(getCurrentTime() + " ERROR " + "The dataframe is empty after removing one-time users")
		raise Exception(getCurrentTime() + " ERROR " + "The dataframe is empty after removing one-time users")
	else:
		print('Events after removing %s time users are %d' %(activity_limit_for_remove, len(df)))
		logMessages.append(getCurrentTime() + " INFO " + 'Events after removing ' + str(activity_limit_for_remove) + ' time users are ' + str(len(df)))

	return df
