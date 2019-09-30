import requests
import ast
import configparser

globalConfigFile = "{{GLOBAL_CONFIG_FILE}}"

config = configparser.ConfigParser()
config.read(globalConfigFile)
strategyApiURL = config['STRATERGY_CONFIG']['STRATEGY_API_URL']
x_ibm_client_ID = config['STRATERGY_CONFIG']['IBM_CLIENT_ID']
x_ibm_client_secret = config['STRATERGY_CONFIG']['IBM_CLIENT_SECRET']
v2v_algoname = config['APP_CONFIG']['V2V_ALGONAME']
b2b_algoname = config['APP_CONFIG']['B2B_ALGONAME']
v2b_algoname = config['APP_CONFIG']['V2B_ALGONAME']
mostpop_algoname = config['APP_CONFIG']['MOSTPOP_ALGONAME']

headerDict = {}
headerDict['x-ibm-client-id'] = x_ibm_client_ID
headerDict['x-ibm-client-secret'] = x_ibm_client_secret
r = requests.get(strategyApiURL, headers = headerDict)
strategyObj = r.text
strategyObj = '[{"id":"af73ca39-c112-4aaa-bcbd-069962fac843","channelTenantId":"cw-502521664-ESSTdDkB","orgId":"502521664","catalogId":"catalog-502521664-ESSTdDkB","zoneId":"871dc011-4491-4d8e-a74e-f3abb054bfef","strategyId":"f51d8300-44a9-4b68-a7d5-56586ddf12bf","primaryStrategyAlgorithm":[{"algorithmName":"Buy to buy","weight":100,"timeFrame":"Past 7 days"}],"fallbackStrategyAlgorithm":[{"algorithmName":"Most popular","category":"All","measureOfPopularity":"View count","weight":100,"timeFrame":"Past 7 days"}],"type":"algorithm"},\
{"id":"af6eacad-2619-4c2f-8fcd-a64deb41faf5","channelTenantId":"cw-502521664-ESSTdDkB","orgId":"502521664","catalogId":"catalog-502521664-ESSTdDkB", "zoneId":"ebe7320d-b1bd-4f4f-abdc-79b4f7057bca","strategyId":"0bbb4e71-a738-4f01-b0f8-7c944e42b167","primaryStrategyAlgorithm":[{"algorithmName":"Buy to buy","weight":100,"timeFrame":"Past 7 days"}],"fallbackStrategyAlgorithm":[{"algorithmName":"None","weight":100}],"type":"algorithm"},\
{"id":"ad4ba4ab-c6ae-42b1-afe7-86842340df00","channelTenantId":"cw-502521664-INKiymyb","catalogId":"catalog3-502521664-INKiymyb", "orgId":"502521664","zoneId":"e81bb0bf-b5d3-4d92-a94d-4cc7bca47356","strategyId":"b85f7055-0aa3-4b62-9ffc-34f4a77f9faf","primaryStrategyAlgorithm":[{"algorithmName":"View to buy","weight":100,"timeFrame":"Past 30 days"}],"fallbackStrategyAlgorithm":[{"algorithmName":"None","weight":100}],"type":"algorithm"},\
{"id":"eb0b80f0-d1fe-41e7-a80f-67003740c435","channelTenantId":"cw-502521664-ESSTdDkB","catalogId":"catalog2-502521664-ESSTdDkB", "orgId":"502521664","zoneId":"21ff67c9-8109-4592-b796-2fd152bc8d5f","strategyId":"ba3fae94-7258-4866-b701-9cad0a23c61d","primaryStrategyAlgorithm":[{"algorithmName":"Most popular","category":"All","measureOfPopularity":"View count","weight":100,"timeFrame":"Past 7 days"}],"fallbackStrategyAlgorithm":[{"algorithmName":"Most popular","category":"cc_cameras","measureOfPopularity":"View count","weight":100,"timeFrame":"Past 30 days"}],"type":"algorithm"},\
{"id":"af6eacad-2619-4c2f-8fcd-a64deb41faf5","channelTenantId":"cw-502521664-ESSTdDkB","orgId":"502521664","catalogId":"catalog2-502521664-ESSTdDkB", "zoneId":"ebe7320d-b1bd-4f4f-abdc-79b4f7057bca","strategyId":"0bbb4e71-a738-4f01-b0f8-7c944e42b167","primaryStrategyAlgorithm":[{"algorithmName":"Buy to buy","weight":100,"timeFrame":"Past 7 days"}],"fallbackStrategyAlgorithm":[{"algorithmName":"None","weight":100}],"type":"algorithm"}]'
strategyObj = ast.literal_eval(strategyObj)

MostPopular = mostpop_algoname
ViewToBuy = v2b_algoname
ViewToView = v2v_algoname
BuyToBuy = b2b_algoname

all_algos = {}
all_algos["instance"] = {}
for element in strategyObj:
    fallbacks = element['fallbackStrategyAlgorithm']
    for instance in fallbacks:
        if (instance["algorithmName"] == MostPopular):
            if "instance" not in all_algos.keys():
                all_algos['instance'] = {}
            if instance["algorithmName"] not in all_algos['instance'].keys():
                all_algos['instance'][instance["algorithmName"]] = {}
            if instance["category"] not in all_algos['instance'][instance["algorithmName"]].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]] = {}
            if instance["measureOfPopularity"] not in all_algos['instance'][instance["algorithmName"]][instance["category"]].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]] = {}
            if element['orgId'] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']] = {}
            if instance['timeFrame'] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']] = {}
            if element["channelTenantId"] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']].keys():
		all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]] = {}
            if element["catalogId"] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element['channelTenantId']].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] = element["zoneId"]
            else:
                zoneList=all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]]
                if(element["zoneId"] not in zoneList):
                    all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] += ',' + element["zoneId"]
        elif (instance["algorithmName"] == ViewToView or instance["algorithmName"] == BuyToBuy or instance["algorithmName"] == ViewToBuy):
            if "instance" not in all_algos.keys():
                all_algos['instance'] = {}
            if instance["algorithmName"] not in all_algos['instance'].keys():
                all_algos['instance'][instance["algorithmName"]] = {}
            if element['orgId'] not in all_algos['instance'][instance["algorithmName"]].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']] = {}
            if instance['timeFrame'] not in all_algos['instance'][instance["algorithmName"]][element['orgId']].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']] = {}
            if element["channelTenantId"] not in all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]] = {}
            if element["catalogId"] not in all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] = element["zoneId"]
            else:
                zoneList = all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]]
                if(element["zoneId"] not in zoneList):
                    all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] += ',' + element["zoneId"]

for element in strategyObj:
    primaries = element['primaryStrategyAlgorithm']
    for instance in primaries:
        if (instance["algorithmName"] == MostPopular):
            if "instance" not in all_algos.keys():
                all_algos['instance'] = {}
            if instance["algorithmName"] not in all_algos['instance'].keys():
                all_algos['instance'][instance["algorithmName"]] = {}
            if instance["category"] not in all_algos['instance'][instance["algorithmName"]].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]] = {}
            if instance["measureOfPopularity"] not in all_algos['instance'][instance["algorithmName"]][instance["category"]].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]] = {}
            if element['orgId'] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']] = {}
            if instance['timeFrame'] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']] = {}
            if element["channelTenantId"] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]] = {}
            if element["catalogId"] not in all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element['channelTenantId']].keys():
                all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] = element["zoneId"]
            else:
                zoneList = all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]]
                if(element["zoneId"] not in zoneList):
                    all_algos['instance'][instance["algorithmName"]][instance["category"]][instance["measureOfPopularity"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] += ',' + element["zoneId"]
        elif (instance["algorithmName"] == ViewToView or instance["algorithmName"] == BuyToBuy or instance["algorithmName"] == ViewToBuy):
            if "instance" not in all_algos.keys():
                all_algos['instance'] = {}
            if instance["algorithmName"] not in all_algos['instance'].keys():
                all_algos['instance'][instance["algorithmName"]] = {}
            if element['orgId'] not in all_algos['instance'][instance["algorithmName"]].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']] = {}
            if instance['timeFrame'] not in all_algos['instance'][instance["algorithmName"]][element['orgId']].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']] = {}
            if element["channelTenantId"] not in all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]] = {}
            if element["catalogId"] not in all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]].keys():
                all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] = element["zoneId"]
            else:
                zoneList = all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]]
                if(element["zoneId"] not in zoneList):
                    all_algos['instance'][instance["algorithmName"]][element['orgId']][instance['timeFrame']][element["channelTenantId"]][element["catalogId"]] += ',' + element["zoneId"]
					
strategyList = []
def getStrategy(dictObj, appendKey): 
    if isinstance(dictObj, dict):
        for key in dictObj.keys():
            try:
                getStrategy(dictObj[key], appendKey + "," + key)
            except Exception as e:
                print("exception....", dictObj)
                raise e
    else:
        appendKey = appendKey + "," + str(dictObj)
        strategyList.append(appendKey)

getStrategy(all_algos, "")

import datetime
launchtime = datetime.datetime.now().strftime("%Y-%m-%d_%H#%M#%S")
mostPopArr = []
newF = open("instance.txt", 'a')
for param in strategyList:
    print(param)
    paramList = param.strip().split(",")
    print(paramList)
    if (paramList[2] == ViewToView or paramList[2] == BuyToBuy or paramList[2] == ViewToBuy):
        if (paramList[2] == ViewToView):
            paramList[2] = "ViewToView"
        if (paramList[2] == ViewToBuy):
            paramList[2] = "ViewToBuy"
        if (paramList[2] == BuyToBuy):
            paramList[2] = "BuyToBuy"
        paramList[4] = paramList[4].split(" ")[1]
	paramList.append(launchtime)
        newF.write(" ".join(paramList[1:]) + "\n")
        print(paramList)
    elif (paramList[2] == MostPopular):
        paramList[6] = paramList[6].split(" ")[1]
        mostPopArr.append(paramList[3:])
mostPopStr = str(mostPopArr)
mostPopStr = mostPopStr.replace(" ", "")
if (len(mostPopArr) != 0):
	mostPopStr += '###'+ launchtime
	newF.write("instance MostPopular " + mostPopStr)
	print('MostPopArr:\n' + mostPopStr)
