import math
import threading
import time
import traceback
from coinmetrics.utils.log import Log
from coinmetrics.utils import pipelines
from coinmetrics.utils.eta import BlockCollectionETA
from coinmetrics.utils.postgres import PostgresAccess
from schema import *
from query import *
from exporter import *
from node import *
from aggregator import *

def postgresFactory(dbConfig):
	return PostgresAccess(dbConfig["host"], dbConfig["database"], dbConfig["user"], dbConfig["password"])

def dbObjectsFactory(asset, db):
	def bitcoinClone(name):
		return [
			lambda db: BitcoinSchema(name, db), 
			lambda db, schema: BitcoinQuery(db, schema), 
			lambda db, schema: BitcoinExporter(db, schema),
			lambda db, query: DailyAggregator(db, query)
		]

	registry = {
		"btc": bitcoinClone("btc"),
		"ltc": bitcoinClone("ltc"),
		"vtc": bitcoinClone("vtc"),
		"doge": bitcoinClone("doge"),
		"dash": bitcoinClone("dash"),
		"dgb": bitcoinClone("dgb"),
		"xvg": bitcoinClone("xvg"),
		"dcr": [
			lambda db: DecredSchema("dcr", db), 
			lambda db, schema: DecredQuery(db, schema), 
			lambda db, schema: DecredExporter(db, schema),
			lambda db, query: DailyAggregator(db, query)
		],
		"zec": [
			lambda db: ZcashSchema("zec", db), 
			lambda db, schema: ZcashQuery(db, schema), 
			lambda db, schema: ZcashExporter(db, schema),
			lambda db, query: DailyAggregator(db, query)
		],
		"pivx": [
			lambda db: PivxSchema("pivx", db), 
			lambda db, schema: PivxQuery(db, schema), 
			lambda db, schema: PivxExporter(db, schema),
			lambda db, query: DailyAggregator(db, query)
		]
	}

	if not asset in registry:
		raise Exception("Unknown asset: %s" % asset)
	else:
		schemaFactory, queryFactory, exporterFactory, aggregatorFactory = registry[asset]
		schema = schemaFactory(db)
		query = queryFactory(db, schema)
		exporter = exporterFactory(db, schema)
		aggregator = aggregatorFactory(db, query)
		return schema, query, exporter, aggregator

def nodeFactory(asset, *args):
	registry = {
		"btc": lambda: BitcoinNode(*args),
		"ltc": lambda: BitcoinNodeBase(*args),
		"vtc": lambda: BitcoinNodeBase(*args),
		"doge": lambda: DogecoinNode(*args),
		"dash": lambda: BitcoinNodeBase(*args),
		"dgb": lambda: BitcoinNodeBase(*args),
		"xvg": lambda: VergeNode(*args),
		"zec": lambda: ZcashNode(*args),
		"pivx": lambda: PivxNode(*args),
		"dcr": lambda: DecredNode(*args),
	}

	if not asset in registry:
		raise Exception("Unknown asset: %s" % asset)
	else:
		return registry[asset]()

def runExport(asset, nodeConfig, dbConfig, loop=False, lag=60 * 60 * 2):
	def proc(db):
		node = nodeFactory(asset, nodeConfig["host"], nodeConfig["port"], nodeConfig["user"], nodeConfig["password"])
		_, query, exporter, _ = dbObjectsFactory(asset, db)

		blockTime = BLOCK_TIMES[asset]
		blockLag = lag / blockTime
		blocksPerWeek = 7 * 24 * 3600 / blockTime

		dbHeight = query.getBlockHeight()
		nodeHeight = node.getBlockCount() - blockLag

		if nodeHeight <= dbHeight:
			print "node height (%d) is smaller than or equal to db height (%d), nothing to do" % (nodeHeight, dbHeight)
			return False, False
		else:
			print "node height: %d, db height: %d, blocks to sync: %d" % (nodeHeight, dbHeight, nodeHeight - dbHeight)
			fromHeight = dbHeight + 1
			toHeight = nodeHeight
			log = Log()

			def load(height, stopSignal):
				return node.getBlock(height)

			def store(blockData, stopSignal):
				exporter.pushBlock(blockData)
				log.msg("saved block at height: %d (%s)" % (blockData.blockHeight, blockData.blockTime))

			heights = [i + fromHeight for i in xrange(toHeight - fromHeight + 1)]
			eta = BlockCollectionETA(toHeight - fromHeight + 1, blocksPerWeek, 10)
			loadPipeline = pipelines.LinearMultithreadedPipeline(8, load, "node")
			storePipeline = pipelines.LinearMultithreadedPipeline(1, store, "db")
			pipelines.OrderingConnector(loadPipeline, storePipeline)
			result = pipelines.runPipelineChain(heights, [loadPipeline, storePipeline], eta, lambda task: "height " + str(task), 64)
			return result

	if not loop:
		proc(postgresFactory(dbConfig))
	else:
		while True:
			keyboardInterrupt = False
			
			try:
				db = postgresFactory(dbConfig)
				result, keyboardInterrupt = proc(db)
			except:
				print traceback.format_exc()
			finally:
				db.close()

			if keyboardInterrupt:
				break

			try:
				time.sleep(300.0)
			except KeyboardInterrupt:
				break


	



	

	


