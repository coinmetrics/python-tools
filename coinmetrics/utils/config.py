import json

def readConfigFile(configFilePath):
	try:
		with open(configFilePath, "r") as f:
			content = f.read()
			jsonConfig = json.loads(content)
			dbConfig = jsonConfig["db"]
			nodesConfig = jsonConfig["nodes"]
			return dbConfig, nodesConfig
	except Exception as e:
		print "failed to read configuration file!"
		print e
		exit()