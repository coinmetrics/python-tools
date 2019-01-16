import time
import traceback
from coinmetrics.utils import pipelines
from coinmetrics.utils.postgres import PostgresAccess
from coinmetrics.bitsql.schema import *
from coinmetrics.bitsql.query import *
from coinmetrics.bitsql.exporter import *
from coinmetrics.bitsql.node import *
from coinmetrics.bitsql.aggregator import *
from coinmetrics.bitsql.omni.node import OmniNode
from coinmetrics.bitsql.omni.schema import OmniSchema
from coinmetrics.bitsql.omni.query import OmniQuery, TetherQuery, MaidSafeCoinQuery
from coinmetrics.bitsql.omni.exporter import OmniExporter
from coinmetrics.bitsql.omni.aggregator import OmniManagedPropertyAggregator, OmniCrowdsalePropertyAggregator
from coinmetrics.utils.execution import executeInParallelSameProc


def postgresFactory(*dbParams):
    return PostgresAccess(*dbParams)


def dbObjectsFactory(asset, db, log):
    def bitcoinClone(name):
        return [
            lambda db: BitcoinSchema(name, db),
            lambda db, schema: BitcoinQuery(db, schema),
            lambda db, schema: BitcoinExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log)
        ]

    registry = {
        "btc": bitcoinClone("btc"),
        "bch": [
            lambda db: BitcoinSchema("bch", db),
            lambda db, schema: BitcoinQuery(db, schema),
            lambda db, schema: BitcoinExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log, datetime(year=2017, month=8, day=3))
        ],
        "btg": [
            lambda db: BitcoinSchema("btg", db),
            lambda db, schema: BitcoinQuery(db, schema),
            lambda db, schema: BitcoinExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log, datetime(year=2017, month=11, day=16))
        ],
        "bsv": [
            lambda db: BitcoinSchema("bsv", db),
            lambda db, schema: BitcoinQuery(db, schema),
            lambda db, schema: BitcoinExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log, datetime(year=2018, month=11, day=16))
        ],
        "ltc": bitcoinClone("ltc"),
        "vtc": bitcoinClone("vtc"),
        "doge": bitcoinClone("doge"),
        "dash": bitcoinClone("dash"),
        "dgb": bitcoinClone("dgb"),
        "xvg": bitcoinClone("xvg"),
        "dcr": [
            lambda db: DecredSchema("dcr", db),
            lambda db, schema: DecredQuery(db, schema),
            lambda db, schema: DecredExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log)
        ],
        "zec": [
            lambda db: ZcashSchema("zec", db),
            lambda db, schema: ZcashQuery(db, schema),
            lambda db, schema: ZcashExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log)
        ],
        "btcp": [
            lambda db: ZcashSchema("btcp", db),
            lambda db, schema: ZcashQuery(db, schema),
            lambda db, schema: ZcashExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log)
        ],
        "pivx": [
            lambda db: PivxSchema("pivx", db),
            lambda db, schema: PivxQuery(db, schema),
            lambda db, schema: PivxExporter(db, schema, log),
            lambda db, query: DailyAggregator(db, query, log)
        ],
        "omnilayer": [
            lambda db: OmniSchema(db),
            lambda db, schema: OmniQuery(db, schema),
            lambda db, schema: OmniExporter(db, schema, log),
            lambda db, query: None
        ],
        "usdt": [
            lambda db: OmniSchema(db),
            lambda db, schema: TetherQuery(db, schema),
            lambda db, schema: None,
            lambda db, query: OmniManagedPropertyAggregator(db, query, log)
        ],
        "maid": [
            lambda db: OmniSchema(db),
            lambda db, schema: MaidSafeCoinQuery(db, schema),
            lambda db, schema: None,
            lambda db, query: OmniCrowdsalePropertyAggregator(db, query, log),
        ]
    }

    if asset not in registry:
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
        "bch": lambda: BitcoinNode(*args),
        "btg": lambda: BitcoinGoldNode(*args),
        "bsv": lambda: BitcoinSvNode(*args),
        "ltc": lambda: BitcoinNodeBase(*args),
        "vtc": lambda: BitcoinNodeBase(*args),
        "doge": lambda: DogecoinNode(*args),
        "dash": lambda: DashNode(*args),
        "dgb": lambda: BitcoinNodeBase(*args),
        "xvg": lambda: VergeNode(*args),
        "zec": lambda: ZcashNode(*args),
        "btcp": lambda: BitcoinPrivateNode(*args),
        "pivx": lambda: PivxNode(*args),
        "dcr": lambda: DecredNode(*args),
        "omnilayer": lambda: OmniNode(*args),
    }

    if asset not in registry:
        raise Exception("Unknown asset: %s" % asset)
    else:
        return registry[asset]()


def runExport(asset, nodeList, dbParams, log, loop=False, lag=60 * 60 * 2, rpcThreads=8):
    def getNodeBlockCount(index, nodeParams):
        node = nodeFactory(asset, *nodeParams)
        try:
            blockCount = node.getBlockCount()
            return (index, blockCount, node)
        except Exception as e:
            log.warning("failed to get block count from node {0}: {1}".format(nodeParams, e))
            raise e

    def pickNode(nodeList, asset):
        args = [(index, nodeParams) for index, nodeParams in enumerate(nodeList)]
        result = [value for value, exception in executeInParallelSameProc(getNodeBlockCount, args) if exception is None]

        if len(result) == 0:
            raise Exception("failed to get block count from any node")
        else:
            pick, bestHeight, bestIndex = None, 0, len(nodeList)
            for index, blockCount, node in result:
                if blockCount > bestHeight:
                    bestHeight = blockCount
                    bestIndex = index
                    pick = node
                elif blockCount == bestHeight:
                    if index < bestIndex:
                        bestIndex = index
                        pick = node
            log.info("picked node: {0}".format(pick))
            return pick, bestHeight

    def proc(db):
        node, nodeHeight = pickNode(nodeList, asset)
        _, query, exporter, _ = dbObjectsFactory(asset, db, log)

        blockTime = BLOCK_TIMES[asset]
        blockLag = lag // blockTime
        blocksPerWeek = 7 * 24 * 3600 // blockTime

        dbHeight = query.getBlockHeight()
        if dbHeight is None:
            dbHeight = -1
        nodeHeight -= blockLag

        if nodeHeight <= dbHeight:
            log.info("node height (%d) is smaller than or equal to db height (%d), nothing to do" % (nodeHeight, dbHeight))
            return False, False
        else:
            log.info("node height: %d, db height: %d, blocks to sync: %d" % (nodeHeight, dbHeight, nodeHeight - dbHeight))
            fromHeight = dbHeight + 1
            toHeight = nodeHeight

            def load(height, stopSignal):
                return node.getBlock(height)

            def store(blockData, stopSignal):
                exporter.pushBlock(blockData)
                log.info("saved block at height: %d (%s)" % (blockData.blockHeight, blockData.blockTime))

            heights = [i + fromHeight for i in range(toHeight - fromHeight + 1)]
            eta = ETA(log, toHeight - fromHeight + 1, blocksPerWeek, 10)
            loadPipeline = pipelines.LinearMultithreadedPipeline(rpcThreads, load, "node")
            storePipeline = pipelines.LinearMultithreadedPipeline(1, store, "db")
            pipelines.OrderingConnector(loadPipeline, storePipeline)
            result = pipelines.runPipelineChain(heights, [loadPipeline, storePipeline], log, eta, lambda task: "height " + str(task), 64)
            return result

    if not loop:
        proc(postgresFactory(*dbParams))
    else:
        while True:
            keyboardInterrupt = False

            try:
                db = postgresFactory(*dbParams)
                result, keyboardInterrupt = proc(db)
            except Exception as e:
                log.critical(traceback.format_exc())
            finally:
                db.close()

            if keyboardInterrupt:
                break

            try:
                time.sleep(30.0)
            except KeyboardInterrupt:
                break
