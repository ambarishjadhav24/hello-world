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

# Class to store Couchbase object and write data to Couchbase
class saveData:
    # Function to initialize Couchbase object
    def __init__(self, couchbUrl, user, password, bucket):
        from couchbase.cluster import Cluster
        from couchbase.cluster import PasswordAuthenticator
        self.cluster = Cluster(couchbUrl)
        self.authenticator = PasswordAuthenticator(user, password)
        self.cluster.authenticate(self.authenticator)
        self.cb = self.cluster.open_bucket(bucket)

    # Function to save data to couchbase
    def saveDataToCouchbase(self, dataDump):
        import time
        import configparser
        import datetime
        key = str(int(time.time()))
        now = datetime.datetime.now()
        day = now.day
        month = now.month
        year = now.year
        hour = now.hour
        minute = now.minute
        timestamp = {}
        timestamp['day'] = day
        timestamp['month'] = month
        timestamp['year'] = year
        timestamp['hour'] = hour
        timestamp['minute'] = minute
        self.cb.upsert(key, dataDump)
