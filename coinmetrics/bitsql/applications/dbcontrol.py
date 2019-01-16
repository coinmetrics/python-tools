import argparse
import logging
from coinmetrics.bitsql import runExport, dbObjectsFactory, postgresFactory
from coinmetrics.bitsql.constants import SUPPORTED_ASSETS
from coinmetrics.utils.arguments import postgres_connection_argument


argParser = argparse.ArgumentParser()
argParser.add_argument("asset", type=str, choices=SUPPORTED_ASSETS)
argParser.add_argument("database", type=postgres_connection_argument, help="Database parameters dbHost:dbPort:dbName:dbUser:dbPassword")
argParser.add_argument("--drop-db", dest="dropDb", action="store_true",
                       help="drop tables that contain data of the given asset")
argParser.add_argument("--add-index", dest="addIndex", action="store_true", help="add indexes to tables")
argParser.add_argument("--drop-index", dest="dropIndex", action="store_true", help="remove table indexes")
argParser.add_argument("--vacuum", action="store_true", help="vacuum outputs table")
args = argParser.parse_args()


logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
appLog = logging.getLogger("bitsql:{0}".format(args.asset))
appLog.setLevel(logging.DEBUG)


if args.dropDb:
    schema, _, _, _ = dbObjectsFactory(args.asset, postgresFactory(*args.database), appLog)
    schema.drop()
elif args.addIndex:
    schema, _, _, _ = dbObjectsFactory(args.asset, postgresFactory(*args.database), appLog)
    schema.addIndexes()
elif args.dropIndex:
    schema, _, _, _ = dbObjectsFactory(args.asset, postgresFactory(*args.database), appLog)
    schema.dropIndexes()
elif args.vacuum:
    schema, _, _, _ = dbObjectsFactory(args.asset, postgresFactory(*args.database), appLog)
    schema.vacuum()
else:
    print("no action chosen, exiting")