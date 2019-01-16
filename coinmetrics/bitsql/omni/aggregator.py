from coinmetrics.bitsql.aggregator import *


class OmniManagedPropertyAggregator(DailyAggregator):

    def createDailyMetrics(self):
        self.addMetric(DailyTxCountStatistic(self.dbAccess, self.query))
        self.addMetric(DailyTxVolumeStatistic(self.dbAccess, self.query))
        self.addMetric(DailyMedianTransactionValueStatistic(self.dbAccess, self.query))
        self.addMetric(DailyActiveAddressesStatistic(self.dbAccess, self.query))
        self.addMetric(DailyRewardStatistic(self.dbAccess, self.query))


class OmniCrowdsalePropertyAggregator(DailyAggregator):

    def createDailyMetrics(self):
        self.addMetric(DailyTxCountStatistic(self.dbAccess, self.query))
        self.addMetric(DailyTxVolumeStatistic(self.dbAccess, self.query))
        self.addMetric(DailyMedianTransactionValueStatistic(self.dbAccess, self.query))
        self.addMetric(DailyActiveAddressesStatistic(self.dbAccess, self.query))
