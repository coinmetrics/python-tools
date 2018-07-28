from coinmetrics.bitsql.aggregator import *

class OmniManagedPropertyAggregator(DailyAggregator):

	def createDailyStatistics(self):
		self.addStatistic(DailyTxCountStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyTxVolumeStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyMedianTransactionValueStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyActiveAddressesStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyRewardStatistic(self.dbAccess, self.query))


class OmniCrowdsalePropertyAggregator(DailyAggregator):

	def createDailyStatistics(self):
		self.addStatistic(DailyTxCountStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyTxVolumeStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyMedianTransactionValueStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyActiveAddressesStatistic(self.dbAccess, self.query))	