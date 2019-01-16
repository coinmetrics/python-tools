import argparse
import logging
from coinmetrics.bitsql import runExport, dbObjectsFactory, postgresFactory
from coinmetrics.bitsql.constants import SUPPORTED_ASSETS
from coinmetrics.utils.arguments import postgres_connection_argument, bitcoin_node_connection_argument


argParser = argparse.ArgumentParser()
argParser.add_argument("asset", type=str, choices=SUPPORTED_ASSETS)
argParser.add_argument("database", type=postgres_connection_argument, help="Database parameters dbHost:dbPort:dbName:dbUser:dbPassword")
argParser.add_argument("nodes", type=bitcoin_node_connection_argument, nargs="+",
                       help="Node parameters host:port:rpcUser:rpcPassword")
argParser.add_argument("--rpcthreads", type=int, default=8, help="Maximum amount of simultaneous RPC requests")
argParser.add_argument("--loop", action="store_true", help="run export continously, in a loop")
args = argParser.parse_args()


logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
appLog = logging.getLogger("bitsql:{0}".format(args.asset))
appLog.setLevel(logging.DEBUG)

runExport(args.asset, args.nodes, args.database, appLog, loop=args.loop, rpcThreads=args.rpcthreads)
