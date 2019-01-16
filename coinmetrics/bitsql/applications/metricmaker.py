import argparse
import logging
import dateutil.parser
import time
from datetime import datetime
from coinmetrics.bitsql import dbObjectsFactory, postgresFactory
from coinmetrics.bitsql.constants import SUPPORTED_ASSETS
from coinmetrics.utils.arguments import postgres_connection_argument


argParser = argparse.ArgumentParser()
argParser.add_argument("assets", type=str, nargs="+", choices=SUPPORTED_ASSETS)
argParser.add_argument("database", type=postgres_connection_argument, help="Database parameters dbHost:dbPort:dbName:dbUser:dbPassword")
argParser.add_argument("--startdate", type=dateutil.parser.parse, default=datetime(2009, 1, 1))
argParser.add_argument("--enddate", type=dateutil.parser.parse, default=datetime.utcnow())
argParser.add_argument("--save", action="store_true", default=False, help="If set, calculcated metrics will be stored in the database")
argParser.add_argument("--force", action="store_true", default=False, help="If set, will trigger re-computation of metrics")
argParser.add_argument("--drop", action="store_true", help="Drop calculated metrics from database")
argParser.add_argument("--metrics", type=str, default=[], nargs="+", help="Metrics on which operation will act, all by default")
argParser.add_argument("--excludemetrics", type=str, default=[], nargs="+", help="Metrics on which operation will not act, none by default")
argParser.add_argument("--list", action="store_true", default=False, help="Will list metrics' names")
argParser.add_argument("--loop", action="store_true", default=False, help="Continously calculate metrics")
args = argParser.parse_args()


logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
appLog = logging.getLogger("bitsql-metric:{0}".format(args.assets))
appLog.setLevel(logging.DEBUG)


def proc(startDate, endDate, save, force):
    for asset in args.assets:
        db = postgresFactory(*args.database)
        _, _, _, aggregator = dbObjectsFactory(asset, db, appLog)

        metrics = aggregator.getMetricNames() if len(args.metrics) == 0 else args.metrics
        metrics = [metric for metric in metrics if len(args.excludemetrics) == 0 or metric not in args.excludemetrics]

        if args.drop:
            aggregator.drop(metrics)
        elif args.list:
            print(aggregator.getMetricNames())
        else:
            aggregator.run(metrics, startDate, endDate, save, force)


if args.loop:
    while True:
        proc(datetime(2009, 1, 1), datetime.utcnow(), True, False)
        appLog.info("going to sleep...")
        time.sleep(300.0)
else:
    proc(args.startdate, args.enddate, args.save, args.force)
