import argparse
from datetime import datetime
from coinmetrics.utils.config import readConfigFile
from coinmetrics.bitsql import dbObjectsFactory, postgresFactory
from coinmetrics.bitsql.constants import SUPPORTED_ASSETS
from dateutil import parser as dateutilParser

argParser = argparse.ArgumentParser()
argParser.add_argument("assets", type=str, nargs="+", choices=SUPPORTED_ASSETS)
argParser.add_argument("--config", default="config.json", help="path to configuration file")
argParser.add_argument("--directory", default="./", help="directory for generated file")
argParser.add_argument("--drop", action="store_true", help="drop aggregated stats tables")
argParser.add_argument("--date", type=str, default="", help="compute aggregates on provided date")
argParser.add_argument("--group", type=str, default="default", help="statistics group to run")
args = argParser.parse_args()
assets = args.assets
drop = args.drop
givenDate = args.date
group = args.group
directory = args.directory
if directory[-1] != "/":
	directory += "/"

dbConfig, _ = readConfigFile(args.config)
db = postgresFactory(dbConfig)

for asset in assets:
	_, _, _, aggregator = dbObjectsFactory(asset, db)
	if drop:
		aggregator.drop(group=group)
	elif len(givenDate) > 0:
		givenDate = dateutilParser.parse(givenDate)
		aggregator.run(givenDate, group)
	else:
		aggregator.run(group=group)
		header, data = aggregator.compile(group=group)
		with open("%s%s.csv" % (directory, asset), "w") as f:
			f.write(",".join(["date"] + header) + "\n")
			for row in data:
				f.write(row[0].strftime('%Y-%m-%d') + ",")
				f.write(",".join([str(value) if value is not None else "" for value in row[1:]]))
				f.write("\n")


