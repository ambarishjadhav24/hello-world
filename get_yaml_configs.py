#!/usr/bin/env python
# coding: utf-8

import yaml
import sys
from datetime import datetime

# Function to get current time in required format
def getCurrentTime():
	return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to parse yaml file and extract hardware configurations
def getYamlConfigs(yamlFile, logMessages):
    import yaml
    import sys
    yamlConfigs = {}
	
    try:
        yamlObject = yaml.load(open(yamlFile))
    except Exception:
        message = 'YAML File not found at path: ' + yamlFile
        logMessages.append(getCurrentTime() + " ERROR " + message + "\n" + str(sys.exc_info()))
    
    try:
        nodeCount = 1
        yamlConfigs['NodeCount'] = nodeCount

        nodeMemory = yamlObject['spec']['template']['spec']['containers'][0]['resources']['requests']['memory']
        yamlConfigs['NodeMemory'] = nodeMemory
		
        nodeCores = yamlObject['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu']
        yamlConfigs['NodeCores'] = nodeCores
		
    except Exception:
        print(str(sys.exc_info()))
        logMessages.append(getCurrentTime() + " ERROR " + str(sys.exc_info()))
    
    return yamlConfigs
