from datetime import timedelta, datetime

class DailyStatistic(object):

	def __init__(self, name, dataType, dbAccess, query):
		self.name = name
		self.dataType = dataType
		self.dbAccess = dbAccess
		self.query = query
		self.asset = query.getSchema().getAsset()
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
		t = datetime.now()
		value = self.calculateForDate(date)
		if save:
			self.save(date, value)
		print "aggregation result for %s on %s: %s (time spent: %s)" % (self.getName(), date, str(value), datetime.now() - t)

	def getDates(self):
		return [row[0] for row in self.dbAccess.queryReturnAll("SELECT date FROM %s" % self.tableName)]

	def save(self, date, value):
		self.dbAccess.queryNoReturnCommit("\
			INSERT INTO " + self.tableName + " (date, value) VALUES (%s, %s)", (date, value))

	def calculateForDate(self, date):
		pass


class SimpleStatistics(DailyStatistic):

	def __init__(self, dbAccess, query):
		super(SimpleStatistics, self).__init__(self.name, self.dataType, dbAccess, query)

	def calculateForDate(self, date):
		return getattr(self.query, self.proc)(date, date + timedelta(days=1))


class DailyTxCountStatistic(SimpleStatistics):
	name = "tx_count"
	dataType = "INTEGER"
	proc = "getTxCountBetween"

class DailyTxVolumeStatistic(SimpleStatistics):
	name = "tx_volume"
	dataType = "DECIMAL(32)"
	proc = "getOutputVolumeBetween"

class DailyActiveAddressesStatistic(SimpleStatistics):
	name = "active_addresses"
	dataType = "INTEGER"
	proc = "getActiveAddressesCountBetween"

class DailyFeesStatistic(SimpleStatistics):
	name = "fees"
	dataType = "BIGINT"
	proc = "getFeesVolumeBetween"

class DailyRewardStatistic(SimpleStatistics):
	name = "reward"
	dataType = "BIGINT"
	proc = "getRewardBetween"

class DailyAverageDifficultyStatistic(SimpleStatistics):
	name = "average_difficulty"
	dataType = "FLOAT"
	proc = "getAverageDifficultyBetween"

class DailyMedianFeeStatistic(SimpleStatistics):
	name = "median_fee"
	dataType = "BIGINT"
	proc = "getMedianFeeBetween"

class DailyMedianTransactionValueStatistic(SimpleStatistics):
	name = "median_tx_value"
	dataType = "BIGINT"
	proc = "getMedianTransactionValueBetween"

class DailyPaymentCountStatistic(SimpleStatistics):
	name = "payment_count"
	dataType = "BIGINT"
	proc = "getPaymentCountBetween"

class DailyBlockSizeStatistic(SimpleStatistics):
	name = "block_size"
	dataType = "BIGINT"
	proc = "getBlockSizeBetween"

class DailyHeuristicalTxVolumeStatistic(SimpleStatistics):
	name = "heuristical_volume"
	dataType = "DECIMAL(32)"
	proc = "getHeuristicalOutputVolumeBetween"


class DailyAggregator(object):

	def __init__(self, dbAccess, query, minDate=None):
		self.dbAccess = dbAccess
		self.query = query
		self.minDate = minDate
		self.groups = {}
		self.addGroup("default")
		self.addGroup("heuristic")
		self.createDailyStatistics()

	def createDailyStatistics(self):
		self.addStatistic(DailyAverageDifficultyStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyTxCountStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyTxVolumeStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyMedianTransactionValueStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyActiveAddressesStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyFeesStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyMedianFeeStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyPaymentCountStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyRewardStatistic(self.dbAccess, self.query))
		self.addStatistic(DailyBlockSizeStatistic(self.dbAccess, self.query))

		self.addStatistic(DailyHeuristicalTxVolumeStatistic(self.dbAccess, self.query), "heuristic")

	def addGroup(self, name):
		if name not in self.groups:
			self.groups[name] = []

	def addStatistic(self, statistic, group="default"):
		assert(group in self.groups)
		self.groups[group].append(statistic)

	def drop(self, group="default"):
		assert(group in self.groups)
		for statistic in self.groups[group]:
			statistic.drop()

	def run(self, givenDate=None, group="default"):
		assert(group in self.groups)
		if givenDate is None:
			minBlockTime = self.query.getMinBlockTime()
			maxBlockTime = self.query.getMaxBlockTime()
			if minBlockTime == 0 or maxBlockTime == 0:
				print "no blocks found for %s" % self.query.getAsset()
				return
			minBlockTime = minBlockTime.replace(hour=0, minute=0, second=0, microsecond=0)
			if self.minDate is not None:
				minBlockTime = max(minBlockTime, self.minDate)
			maxBlockTime = maxBlockTime.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
			dateSet = set([minBlockTime + timedelta(days=i) for i in xrange((maxBlockTime - minBlockTime).days + 1)])
			shouldSave = True
		else:
			dateSet = set([givenDate])
			shouldSave = False

		statistics = self.groups[group]
		datesToStatMap = {}
		for statistic in self.groups[group]:
			doneDates = statistic.getDates()
			missingDates = dateSet.difference(doneDates)
			for missingDate in missingDates:
				if not missingDate in datesToStatMap:
					datesToStatMap[missingDate] = []
				datesToStatMap[missingDate].append(statistic)

		datesAndStats = [(missingDate, statList) for missingDate, statList in datesToStatMap.iteritems()]
		datesAndStats = sorted(datesAndStats, key=lambda pair: pair[0])
		for missingDate, statList in datesAndStats:
			for stat in statList:
				stat.runOn(missingDate, shouldSave)

	def compile(self, group="default"):
		assert(group in self.groups)
		statistics = self.groups[group]
		selectText = "coalesce(" + ", ".join([s.getTableName() + ".date" for s in statistics]) + ") as d, "
		selectText += ", ".join([s.getTableName() + ".value" for s in statistics])
		joinText = statistics[0].getTableName()
		for i in xrange(len(statistics) - 1):
			tableName = statistics[i + 1].getTableName()
			joinText += " FULL OUTER JOIN " + tableName + " ON " + tableName + ".date=" + statistics[0].getTableName() + ".date"
		queryText = "SELECT %s FROM %s ORDER BY d ASC" % (selectText, joinText)
		return [s.getName() for s in statistics], self.query.run(queryText)




