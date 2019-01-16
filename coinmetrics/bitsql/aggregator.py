from datetime import timedelta, datetime
from coinmetrics.utils.eta import ETA
from coinmetrics.utils.timeutil import alignDateToInterval


class DailyStatistic(object):

    def __init__(self, name, dataType, dbAccess, query):
        self.name = name
        self.dataType = dataType
        self.dbAccess = dbAccess
        self.query = query
        self.asset = query.getAsset()
        self.init()

    def getName(self):
        return self.name

    def getTableName(self):
        return self.tableName

    def init(self):
        self.tableName = "statistic_" + self.name + "_" + self.asset
        self.dbAccess.queryNoReturnCommit("\
            CREATE TABLE IF NOT EXISTS %s (date TIMESTAMP PRIMARY KEY, value %s)" % (self.tableName, self.dataType))

    def drop(self):
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % self.tableName)

    def runOn(self, date, save=True):
        value = self.calculateForDate(date)
        if save:
            self.save(date, value)
        return value

    def getDates(self):
        return [row[0] for row in self.dbAccess.queryReturnAll("SELECT date FROM %s" % self.tableName)]

    def save(self, date, value):
        self.dbAccess.queryNoReturnCommit("""
            INSERT INTO {0} (date, value) VALUES (%s, %s)
            ON CONFLICT ON CONSTRAINT {0}_pkey DO UPDATE SET value=EXCLUDED.value""".format(self.tableName), (date, value))

    def calculateForDate(self, date):
        pass


class SimpleStatistic(DailyStatistic):

    def __init__(self, dbAccess, query):
        super(SimpleStatistic, self).__init__(self.name, self.dataType, dbAccess, query)

    def calculateForDate(self, date):
        return getattr(self.query, self.proc)(date, date + timedelta(days=1))


class DailyTxCountStatistic(SimpleStatistic):
    name = "tx_count"
    dataType = "INTEGER"
    proc = "getTxCountBetween"

class DailyTxVolumeStatistic(SimpleStatistic):
    name = "tx_volume"
    dataType = "DECIMAL(32)"
    proc = "getOutputVolumeBetween"

class DailyActiveAddressesStatistic(SimpleStatistic):
    name = "active_addresses"
    dataType = "INTEGER"
    proc = "getActiveAddressesCountBetween"

class DailyFeesStatistic(SimpleStatistic):
    name = "fees"
    dataType = "BIGINT"
    proc = "getFeesVolumeBetween"

class DailyRewardStatistic(SimpleStatistic):
    name = "reward"
    dataType = "BIGINT"
    proc = "getRewardBetween"

class DailyAverageDifficultyStatistic(SimpleStatistic):
    name = "average_difficulty"
    dataType = "FLOAT"
    proc = "getAverageDifficultyBetween"

class DailyMedianFeeStatistic(SimpleStatistic):
    name = "median_fee"
    dataType = "BIGINT"
    proc = "getMedianFeeBetween"

class DailyMedianTransactionValueStatistic(SimpleStatistic):
    name = "median_tx_value"
    dataType = "BIGINT"
    proc = "getMedianTransactionValueBetween"

class DailyPaymentCountStatistic(SimpleStatistic):
    name = "payment_count"
    dataType = "BIGINT"
    proc = "getPaymentCountBetween"

class DailyBlockSizeStatistic(SimpleStatistic):
    name = "block_size"
    dataType = "BIGINT"
    proc = "getBlockSizeBetween"

class DailyHeuristicalTxVolumeStatistic(SimpleStatistic):
    name = "heuristical_volume"
    dataType = "DECIMAL(32)"
    proc = "getHeuristicalOutputVolumeBetween"

class DailyBlockCountStatistic(SimpleStatistic):
    name = "block_count"
    dataType = "BIGINT"
    proc = "getBlockCountBetween"

class Daily1YCirculatingSupplyStatistic(SimpleStatistic):
    name = "1y_circulating_supply"
    dataType = "DECIMAL(32)"
    proc = "get1YCirculatingSupplyBetween"

class Daily180DCirculatingSupplyStatistic(SimpleStatistic):
    name = "180d_circulating_supply"
    dataType = "DECIMAL(32)"
    proc = "get180DCirculatingSupplyBetween"

class Daily30DCirculatingSupplyStatistic(SimpleStatistic):
    name = "30d_circulating_supply"
    dataType = "DECIMAL(32)"
    proc = "get30DCirculatingSupplyBetween"

class DailyTotalSupplyStatistic(SimpleStatistic):
    name = "total_supply"
    dataType = "DECIMAL(32)"
    proc = "getTotalSupplyBetween"

class DailyNaive30DCirculatingSupplyStatistic(SimpleStatistic):
    name = "30d_naive_circulating_supply"
    dataType = "DECIMAL(32)"
    proc = "get30DNaiveCirculatingSupplyBetween"


class DailyAggregator(object):

    def __init__(self, dbAccess, query, log, minDate=None):
        self.dbAccess = dbAccess
        self.query = query
        self.log = log
        self.minDate = minDate
        self.metrics = []

        self.createDailyMetrics()

    def createDailyMetrics(self):
        self.addMetric(DailyAverageDifficultyStatistic(self.dbAccess, self.query))
        self.addMetric(DailyTxCountStatistic(self.dbAccess, self.query))
        self.addMetric(DailyTxVolumeStatistic(self.dbAccess, self.query))
        self.addMetric(DailyHeuristicalTxVolumeStatistic(self.dbAccess, self.query))
        self.addMetric(DailyMedianTransactionValueStatistic(self.dbAccess, self.query))
        self.addMetric(DailyActiveAddressesStatistic(self.dbAccess, self.query))
        self.addMetric(DailyFeesStatistic(self.dbAccess, self.query))
        self.addMetric(DailyMedianFeeStatistic(self.dbAccess, self.query))
        self.addMetric(DailyPaymentCountStatistic(self.dbAccess, self.query))
        self.addMetric(DailyRewardStatistic(self.dbAccess, self.query))
        self.addMetric(DailyBlockSizeStatistic(self.dbAccess, self.query))
        self.addMetric(DailyBlockCountStatistic(self.dbAccess, self.query))
        self.addMetric(Daily1YCirculatingSupplyStatistic(self.dbAccess, self.query))
        self.addMetric(Daily180DCirculatingSupplyStatistic(self.dbAccess, self.query))
        self.addMetric(Daily30DCirculatingSupplyStatistic(self.dbAccess, self.query))
        self.addMetric(DailyTotalSupplyStatistic(self.dbAccess, self.query))
        # can be useful for diagnosis
        # self.addMetric(DailyNaive30DCirculatingSupplyStatistic(self.dbAccess, self.query))

    def getMetricNames(self):
        return [metric.getName() for metric in self.metrics]

    def addMetric(self, metric):
        self.metrics.append(metric)

    def run(self, metricNames, startDate, endDate, shouldSave, forceRecomputation):
        minBlockTime, maxBlockTime = self._getBlockchainTimeBounds()
        minComputeTime = max(minBlockTime, alignDateToInterval(startDate, timedelta(days=1)))
        maxComputeTime = min(maxBlockTime, alignDateToInterval(endDate, timedelta(days=1)))
        computeDateSet = set([minComputeTime + timedelta(days=i) for i in range((maxComputeTime - minComputeTime).days + 1)])

        datesToStatMap = {}
        for metric in self.metrics:
            if metric.getName() in metricNames:
                doneDates = {} if forceRecomputation else metric.getDates()
                missingDates = computeDateSet.difference(doneDates)
                for missingDate in missingDates:
                    if missingDate not in datesToStatMap:
                        datesToStatMap[missingDate] = []
                    datesToStatMap[missingDate].append(metric)

        datesAndStats = [(missingDate, statList) for missingDate, statList in datesToStatMap.items()]
        datesAndStats = sorted(datesAndStats, key=lambda pair: pair[0])
        if len(datesAndStats) == 0:
            self.log.info("no metrics to calculate")
            return

        eta = ETA(self.log, len(datesAndStats), 60, 10)
        for missingDate, statList in datesAndStats:
            eta.workStarted()
            self.log.info("date: %s" % missingDate)
            for stat in statList:
                t = datetime.now()
                value = stat.runOn(missingDate, shouldSave)
                self.log.info("aggregation result for %s on %s: %s (time spent: %s)" % (stat.getName(), missingDate, str(value), datetime.now() - t))
            eta.workFinished(1)

    def drop(self, metricNames):
        for metric in self.metrics:
            if metric.getName() in metricNames:
                metric.drop()

    def _getBlockchainTimeBounds(self):
        minBlockTime = self.query.getMinBlockTime()
        maxBlockTime = self.query.getMaxBlockTime()
        if minBlockTime == 0 or maxBlockTime == 0:
            raise ValueError("no blocks found for %s" % self.query.getAsset())

        minBlockTime = minBlockTime.replace(hour=0, minute=0, second=0, microsecond=0)
        if self.minDate is not None:
            minBlockTime = max(minBlockTime, self.minDate)

        maxBlockTime = maxBlockTime.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        return minBlockTime, maxBlockTime
