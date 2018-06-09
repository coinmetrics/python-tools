from constants import *

class BitcoinSchema(object):

	def __init__(self, asset, dbAccess):
		self.asset = asset
		self.dbAccess = dbAccess
		self.init()

	def getAsset(self):
		return self.asset

	def getBlocksTableName(self):
		return self.blocksTableName

	def getTransactionsTableName(self):
		return self.transactionsTableName

	def getOutputsTableName(self):
		return self.outputsTableName

	def init(self):
		self.blocksTableName = "blocks_" + self.asset
		self.transactionsTableName = "transactions_" + self.asset
		self.outputsTableName = "outputs_" + self.asset

		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			block_hash DECIMAL(%s) PRIMARY KEY, \
			block_height INTEGER, \
			block_size INTEGER, \
			block_time TIMESTAMP, \
			block_difficulty DOUBLE PRECISION, \
			block_chainwork DECIMAL(%s) \
			)" % (self.blocksTableName, HASH_PRECISION, CHAINWORK_PRECISION))

		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			tx_hash DECIMAL(%s) PRIMARY KEY, \
			tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			tx_size INTEGER, \
			tx_time TIMESTAMP, \
			tx_coinbase BOOLEAN \
			)" % (self.transactionsTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName))

		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			output_tx_hash DECIMAL(%s) references %s(tx_hash) ON DELETE CASCADE, \
			output_index INTEGER, \
			output_type SMALLINT, \
			output_addresses VARCHAR(%s)[], \
			output_script BYTEA, \
			output_value_satoshi DECIMAL(%s), \
			output_spending_tx_hash DECIMAL(%s) references %s(tx_hash) ON DELETE CASCADE, \
			output_time_created TIMESTAMP, \
			output_time_spent TIMESTAMP, \
			PRIMARY KEY(output_tx_hash, output_index)\
			)" % (self.outputsTableName, HASH_PRECISION, self.transactionsTableName, MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION, HASH_PRECISION, self.transactionsTableName))

	def drop(self):
		self.dropIndexes()
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.outputsTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.transactionsTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.blocksTableName,))

	def addIndexes(self):
		self.dbAccess.queryNoReturnCommit("CREATE INDEX block_time_index_%s ON blocks_%s(block_time)" % (self.asset, self.asset))
		self.dbAccess.queryNoReturnCommit("CREATE INDEX block_height_index_%s ON blocks_%s(block_height)" % (self.asset, self.asset))
		self.dbAccess.queryNoReturnCommit("CREATE INDEX tx_time_index_%s ON transactions_%s(tx_time)" % (self.asset, self.asset))
		self.dbAccess.queryNoReturnCommit("CREATE INDEX output_time_spent_index_%s ON outputs_%s(output_time_spent)" % (self.asset, self.asset))
		self.dbAccess.queryNoReturnCommit("CREATE INDEX output_time_created_index_%s ON outputs_%s(output_time_created)" % (self.asset, self.asset))

	def dropIndexes(self):
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS output_time_created_index_%s" % (self.asset,))
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS output_time_spent_index_%s" % (self.asset,))
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS tx_time_index_%s" % (self.asset,))
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS block_height_index_%s" % (self.asset,))
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS block_time_index_%s" % (self.asset,))

	def vacuum(self):
		isolationLevel = self.dbAccess.connection.isolation_level
		self.dbAccess.connection.set_isolation_level(0)
		self.dbAccess.queryNoReturnNoCommit("VACUUM %s" % self.getOutputsTableName())
		self.dbAccess.connection.set_isolation_level(isolationLevel)


class ZcashSchema(BitcoinSchema):

	def getJoinSplitsTableName(self):
		return self.joinSplitsTableName

	def init(self):
		super(ZcashSchema, self).init()
		self.joinSplitsTableName = "joinsplits_" + self.asset
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			id SERIAL PRIMARY KEY, \
			joinsplit_tx_hash DECIMAL(%s) references %s(tx_hash), \
			joinsplit_value_old DECIMAL(%s), \
			joinsplit_value_new DECIMAL(%s), \
			joinsplit_time TIMESTAMP\
			)" % (self.joinSplitsTableName, HASH_PRECISION, self.transactionsTableName, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

	def drop(self):
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.joinSplitsTableName,))
		super(ZcashSchema, self).drop()

	def addIndexes(self):
		super(ZcashSchema, self).addIndexes()
		self.dbAccess.queryNoReturnCommit("CREATE INDEX joinsplit_time_index_%s ON joinsplits_%s(joinsplit_time)" % (self.asset, self.asset))

	def dropIndexes(self):
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS joinsplit_time_index_%s" % (self.asset,))
		super(ZcashSchema, self).dropIndexes()


class PivxSchema(BitcoinSchema):

	def getZerocoinMintsTableName(self):
		return self.zerocoinMintsTableName

	def getZerocoinSpendsTableName(self):
		return self.zerocoinSpendsTableName

	def init(self):
		super(PivxSchema, self).init()
		self.zerocoinMintsTableName = "zerocoin_mints_" + self.asset
		self.zerocoinSpendsTableName = "zerocoin_spends_" + self.asset

		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			id SERIAL PRIMARY KEY, \
			zerocoin_mint_tx_hash DECIMAL(%s) references %s(tx_hash), \
			zerocoin_mint_value DECIMAL(%s), \
			zerocoin_mint_time TIMESTAMP \
			)" % (self.zerocoinMintsTableName, HASH_PRECISION, self.transactionsTableName, OUTPUT_VALUE_PRECISION))

		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			id SERIAL PRIMARY KEY, \
			zerocoin_spend_tx_hash DECIMAL(%s) references %s(tx_hash), \
			zerocoin_spend_value DECIMAL(%s), \
			zerocoin_spend_time TIMESTAMP \
			)" % (self.zerocoinSpendsTableName, HASH_PRECISION, self.transactionsTableName, OUTPUT_VALUE_PRECISION))

	def drop(self):
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.zerocoinSpendsTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.zerocoinMintsTableName,))
		super(PivxSchema, self).drop()

	def addIndexes(self):
		super(PivxSchema, self).addIndexes()
		self.dbAccess.queryNoReturnCommit("CREATE INDEX zerocoin_mint_time_index_%s ON zerocoin_mints_%s(zerocoin_mint_time)" % (self.asset, self.asset))
		self.dbAccess.queryNoReturnCommit("CREATE INDEX zerocoin_spend_time_index_%s ON zerocoin_spends_%s(zerocoin_spend_time)" % (self.asset, self.asset))

	def dropIndexes(self):
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS zerocoin_spend_time_index_%s" % (self.asset,))
		self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS zerocoin_mint_time_index_%s" % (self.asset,))
		super(PivxSchema, self).dropIndexes()


class DecredSchema(BitcoinSchema):

	def init(self):
		super(DecredSchema, self).init()
		self.dbAccess.queryNoReturnCommit("ALTER TABLE %s ADD COLUMN IF NOT EXISTS tx_vote BOOLEAN" % self.transactionsTableName)
		self.dbAccess.queryNoReturnCommit("ALTER TABLE %s ADD COLUMN IF NOT EXISTS tx_ticket BOOLEAN" % self.transactionsTableName)