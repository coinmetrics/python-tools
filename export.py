import argparse
from coinmetrics.utils.config import readConfigFile
from coinmetrics.bitsql import runExport, dbObjectsFactory, postgresFactory
from coinmetrics.bitsql.constants import SUPPORTED_ASSETS

argParser = argparse.ArgumentParser()
argParser.add_argument("asset", type=str, choices=SUPPORTED_ASSETS)
argParser.add_argument("--drop-db", dest="dropDb", action="store_true", help="drop tables that contain data of the given asset")
argParser.add_argument("--add-index", dest="addIndex", action="store_true", help="add indexes to tables")
argParser.add_argument("--drop-index", dest="dropIndex", action="store_true", help="remove table indexes")
argParser.add_argument("--loop", action="store_true", help="run export continously, in a loop")
argParser.add_argument("--config", default="config.json", help="path to configuration file")
argParser.add_argument("--vacuum", action="store_true", help="vacuum outputs table")
args = argParser.parse_args()
asset = args.asset
addIndex = args.addIndex
dropIndex = args.dropIndex
dropDb = args.dropDb
loop = args.loop
vacuum = args.vacuum
configFilePath = args.config

dbConfig, nodesConfig = readConfigFile(configFilePath)

if dropDb:
	schema, _, _, _ = dbObjectsFactory(asset, postgresFactory(dbConfig))
	schema.drop()
elif addIndex:
	schema, _, _, _ = dbObjectsFactory(asset, postgresFactory(dbConfig))
	schema.addIndexes()
elif dropIndex:
	schema, _, _, _ = dbObjectsFactory(asset, postgresFactory(dbConfig))
	schema.dropIndexes()
elif vacuum:
	schema, _, _, _ = dbObjectsFactory(asset, postgresFactory(dbConfig))
	schema.vacuum()
else:
	runExport(asset, nodesConfig[asset], dbConfig, loop=loop)