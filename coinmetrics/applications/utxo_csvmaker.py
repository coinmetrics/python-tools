import re
import time
import argparse
import logging
import dateutil.parser
from datetime import datetime
from collections import defaultdict
from coinmetrics.bitsql import dbObjectsFactory, postgresFactory
from coinmetrics.bitsql.constants import SUPPORTED_ASSETS
from coinmetrics.utils.arguments import postgres_connection_argument
from coinmetrics.utils.file import AtomicallySwappableFile


argParser = argparse.ArgumentParser()
argParser.add_argument("assets", type=str, nargs="+", choices=SUPPORTED_ASSETS)
argParser.add_argument("database", type=postgres_connection_argument, help="Database parameters dbHost:dbPort:dbName:dbUser:dbPassword")
argParser.add_argument("--directory", default="./", help="directory for generated file")
argParser.add_argument("--metrics", type=str, default=[], nargs="+", help="Metrics on which operation will act, all by default")
argParser.add_argument("--excludemetrics", type=str, default=[], nargs="+", help="Metrics which will not be included into CSV, none by default")
argParser.add_argument("--loop", action="store_true", default=False, help="Continously rebuild CSV file from metrics")
args = argParser.parse_args()


directory = args.directory
if directory[-1] != "/":
    directory += "/"


logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
appLog = logging.getLogger("bitsql-gen:{0}".format(args.assets))
appLog.setLevel(logging.DEBUG)


def proc():
    db = postgresFactory(*args.database)
    tableNames = sorted(db.getTableNames())

    for asset in args.assets:
        pattern = re.compile("^statistic_([a-z_]+)_{0}$".format(asset))
        dates = defaultdict(dict)
        metricNames = set()

        for tableName in tableNames:
            match = pattern.match(tableName)
            if match is not None and match.group(1) not in args.excludemetrics and (len(args.metrics) == 0 or match.group(1) in args.metrics):
                metricName = match.group(1)
                metricNames.add(metricName)
                dateValues = db.queryReturnAll("""SELECT date, value FROM {0}""".format(tableName))
                for date, value in dateValues:
                    dates[date][metricName] = value

        datesList = sorted([(date, metrics) for date, metrics in dates.items()], key=lambda pair: pair[0])

        csvContent = []
        metricList = sorted(list(metricNames) if len(args.metrics) == 0 else args.metrics)
        header = ",".join(["date"] + metricList)
        csvContent.append(header + "\n")
        for date, metrics in datesList:
            csvContent.append(date.strftime('%Y-%m-%d') + ",")
            values = []
            for metric in metricList:
                if metric in metrics:
                    metricValue = metrics[metric]
                    metricValueString = str(metricValue) if metricValue is not None else ""
                    values.append(metricValueString)
                else:
                    values.append("")
            csvContent.append(",".join(values) + "\n")

        AtomicallySwappableFile("{0}.csv".format(asset), directory).update("".join(csvContent))


if args.loop:
    while True:
        proc()
        appLog.info("going to sleep...")
        time.sleep(300.0)
else:
    proc()
