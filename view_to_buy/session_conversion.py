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

# Function to convert events data into day wise sessions for each user and filter according to timeframe
def session_convert (df, timestampCol, eventCol, viewEvent, buyEvent, logMessages, timeFrame, timeframe1, timeframe7, timeframe30):
    from get_yaml_configs import getCurrentTime
    import sys
    import datetime
    
    df["V2BSession"] = df[timestampCol].apply(lambda x: x.date())    
    df = df.drop([timestampCol],axis=1)
    print(df.head(10))
    dateCol = df['V2BSession'].unique().tolist()
    if (timeFrame == timeframe1) or (timeFrame == timeframe7) or (timeFrame == timeframe30):
        logMessages.append(getCurrentTime() + " INFO " + "Timeframe chosen as: " + str(timeFrame) + " days.")
        end_date = df['V2BSession'].max()
        print("enddate:" + str(end_date))
        timeFrameDates = [(end_date - datetime.timedelta(days=x)) for x in range(1, int(timeFrame)+1)]
        print("Dates in File: {"  + str(timeFrameDates) + "}.")
        filter_dates = [dates for dates in timeFrameDates if dates in dateCol]
        print("Dates post filter: {"  + str(filter_dates) + "}.")
        df = df[df["V2BSession"].isin(filter_dates)]
        if (len(filter_dates) == int(timeFrame)):
            logMessages.append(getCurrentTime() + " INFO " + timeFrame  +" days of Behavioural data filtered.")
        else:
            message = "Behavioural Data available only for: " + str(len(filter_dates)) + " days."
            logMessages.append(getCurrentTime() + " WARNING " + message)
    else:
        message = "Timeframe can only be 1 or 7 or 30"
        logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
        raise Exception(message)
    mandatoryEvents = [buyEvent,viewEvent]
    eventsAvailable = df[eventCol].unique().tolist()
    logMessages.append(getCurrentTime() + " INFO " + "Events available in Behavioural Data: {"  + ",".join(eventsAvailable) + "}.")
    print(eventsAvailable)
    for event in mandatoryEvents:
        if not bool(event in eventsAvailable):
            message = '"' + event + '" events are not found in event data after filter applied.'
            logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
            raise Exception(message)
    df = df[df[eventCol].isin(mandatoryEvents)]
    df = df.drop_duplicates()
    if bool(df.empty):
        message = "Post transformation data frame getting empty."
        logMessages.append(getCurrentTime() + " ERROR " + message)
        raise Exception(message)
    return (df)
