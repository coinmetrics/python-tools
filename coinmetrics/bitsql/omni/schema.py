from coinmetrics.bitsql.constants import HASH_PRECISION, MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION

class OmniSchema(object):

	def __init__(self, dbAccess):
		self.dbAccess = dbAccess
		self.init()

	def getBlocksTableName(self):
		return self.blocksTableName

	def getSimpleSendTxTableName(self):
		return self.simpleSendTxTableName

	def getSimpleSendTxPrefix(self):
		return "simple_send_tx"

	def getSellForBitcoinTxTableName(self):
		return self.sellForBitcoinTxTableName

	def getSellForBitcoinTxPrefix(self):
		return "sell_for_bitcoin_tx"

	def getAcceptSellForBitcoinTxTableName(self):
		return self.acceptSellForBitcoinTxTableName

	def getAcceptSellForBitcoinTxPrefix(self):
		return "accept_sell_for_bitcoin_tx"

	def getDexPurchaseTxTableName(self):
		return self.dexPurchaseTxTableName

	def getDexPurchaseTxPrefix(self):
		return "dex_purchase_tx"

	def getCreateFixedPropertyTxTableName(self):
		return self.createFixedPropertyTxTableName

	def getCreateFixedPropertyTxPrefix(self):
		return "create_fixed_property_tx"

	def getCreateCrowdsalePropertyTxTableName(self):
		return self.createCrowdsalePropertyTxTableName

	def getCreateCrowdsalePropertyTxPrefix(self):
		return "create_crowdsale_property_tx"

	def getCloseCrowdsaleTxTableName(self):
		return self.closeCrowdsaleTxTableName

	def getCloseCrowdsaleTxPrefix(self):
		return "close_crowdsale_tx"

	def getSendOwnersTxTableName(self):
		return self.sendOwnersTxTableName
	
	def getSendOwnersTxPrefix(self):
		return "send_owners_tx"

	def getCreateManagedPropertyTxTableName(self):
		return self.createManagedPropertyTxTableName

	def getCreateManagedPropertyTxPrefix(self):
		return "create_managed_property_tx"

	def getGrantTokensTxTableName(self):
		return self.grantTokensTxTableName

	def getGrantTokensTxPrefix(self):
		return "grant_tokens_tx"

	def getRevokeTokensTxTableName(self):
		return self.revokeTokensTxTableName

	def getRevokeTokensTxPrefix(self):
		return "revoke_tokens_tx"

	def getSendAllTxTableName(self):
		return self.sendAllTxTableName

	def getSendAllTxPrefix(self):
		return "send_all_tx"

	def init(self):
		self.blocksTableName = "blocks_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			block_hash DECIMAL(%s) PRIMARY KEY, \
			block_height INTEGER \
			)" % (self.blocksTableName, HASH_PRECISION))

		self.simpleSendTxTableName = "simple_send_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			simple_send_tx_hash DECIMAL(%s) PRIMARY KEY, \
			simple_send_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			simple_send_tx_time TIMESTAMP, \
			simple_send_tx_sending_address VARCHAR(%s), \
			simple_send_tx_receiving_address VARCHAR(%s), \
			simple_send_tx_property_id BIGINT, \
			simple_send_tx_amount DECIMAL(%s), \
			simple_send_tx_fee DECIMAL(%s) \
			)" % (self.simpleSendTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.sendAllTxTableName = "send_all_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			send_all_tx_hash DECIMAL(%s), \
			send_all_tx_index INTEGER, \
			send_all_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			send_all_tx_time TIMESTAMP, \
			send_all_tx_sending_address VARCHAR(%s), \
			send_all_tx_receiving_address VARCHAR(%s), \
			send_all_tx_property_id BIGINT, \
			send_all_tx_amount DECIMAL(%s), \
			send_all_tx_fee DECIMAL(%s), \
			PRIMARY KEY(send_all_tx_hash, send_all_tx_index) \
			)" % (self.sendAllTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.sendOwnersTxTableName = "send_owners_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			send_owners_tx_hash DECIMAL(%s) PRIMARY KEY, \
			send_owners_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			send_owners_tx_time TIMESTAMP, \
			send_owners_tx_sending_address VARCHAR(%s), \
			send_owners_tx_property_id BIGINT, \
			send_owners_tx_amount DECIMAL(%s), \
			send_owners_tx_fee DECIMAL(%s) \
			)" % (self.sendOwnersTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))
		
		self.sellForBitcoinTxTableName = "sell_for_bitcoin_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			sell_for_bitcoin_tx_hash DECIMAL(%s) PRIMARY KEY, \
			sell_for_bitcoin_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			sell_for_bitcoin_tx_time TIMESTAMP, \
			sell_for_bitcoin_tx_sending_address VARCHAR(%s), \
			sell_for_bitcoin_tx_property_id BIGINT, \
			sell_for_bitcoin_tx_amount DECIMAL(%s), \
			sell_for_bitcoin_tx_fee DECIMAL(%s), \
			sell_for_bitcoin_tx_fee_required DECIMAL(%s), \
			sell_for_bitcoin_tx_bitcoin_desired DECIMAL(%s), \
			sell_for_bitcoin_tx_action SMALLINT \
			)" % (self.sellForBitcoinTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.acceptSellForBitcoinTxTableName = "accept_sell_for_bitcoin_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			accept_sell_for_bitcoin_tx_hash DECIMAL(%s) PRIMARY KEY, \
			accept_sell_for_bitcoin_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			accept_sell_for_bitcoin_tx_time TIMESTAMP, \
			accept_sell_for_bitcoin_tx_sending_address VARCHAR(%s), \
			accept_sell_for_bitcoin_tx_property_id BIGINT, \
			accept_sell_for_bitcoin_tx_amount DECIMAL(%s), \
			accept_sell_for_bitcoin_tx_fee DECIMAL(%s), \
			accept_sell_for_bitcoin_tx_receiving_address VARCHAR(%s) \
			)" % (self.acceptSellForBitcoinTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION, MAX_ADDRESS_LENGTH))

		self.dexPurchaseTxTableName = "dex_purchase_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			dex_purchase_tx_hash DECIMAL(%s), \
			dex_purchase_tx_index INTEGER, \
			dex_purchase_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			dex_purchase_tx_time TIMESTAMP, \
			dex_purchase_tx_sending_address VARCHAR(%s), \
			dex_purchase_tx_property_id BIGINT, \
			dex_purchase_tx_amount_bought DECIMAL(%s), \
			dex_purchase_tx_amount_paid DECIMAL(%s), \
			dex_purchase_tx_receiving_address VARCHAR(%s), \
			PRIMARY KEY(dex_purchase_tx_hash, dex_purchase_tx_index) \
			)" % (self.dexPurchaseTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION, MAX_ADDRESS_LENGTH))

		self.createFixedPropertyTxTableName = "create_fixed_property_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			create_fixed_property_tx_hash DECIMAL(%s) PRIMARY KEY, \
			create_fixed_property_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			create_fixed_property_tx_time TIMESTAMP, \
			create_fixed_property_tx_sending_address VARCHAR(%s), \
			create_fixed_property_tx_property_id BIGINT, \
			create_fixed_property_tx_property_type SMALLINT, \
			create_fixed_property_tx_amount DECIMAL(%s), \
			create_fixed_property_tx_fee DECIMAL(%s) \
			)" % (self.createFixedPropertyTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.createManagedPropertyTxTableName = "create_managed_property_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			create_managed_property_tx_hash DECIMAL(%s) PRIMARY KEY, \
			create_managed_property_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			create_managed_property_tx_time TIMESTAMP, \
			create_managed_property_tx_sending_address VARCHAR(%s), \
			create_managed_property_tx_property_id BIGINT, \
			create_managed_property_tx_property_type SMALLINT, \
			create_managed_property_tx_fee DECIMAL(%s) \
			)" % (self.createManagedPropertyTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION))

		self.grantTokensTxTableName = "grant_tokens_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			grant_tokens_tx_hash DECIMAL(%s) PRIMARY KEY, \
			grant_tokens_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			grant_tokens_tx_time TIMESTAMP, \
			grant_tokens_tx_sending_address VARCHAR(%s), \
			grant_tokens_tx_receiving_address VARCHAR(%s), \
			grant_tokens_tx_property_id BIGINT, \
			grant_tokens_tx_amount DECIMAL(%s), \
			grant_tokens_tx_fee DECIMAL(%s) \
			)" % (self.grantTokensTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.revokeTokensTxTableName = "revoke_tokens_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			revoke_tokens_tx_hash DECIMAL(%s) PRIMARY KEY, \
			revoke_tokens_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			revoke_tokens_tx_time TIMESTAMP, \
			revoke_tokens_tx_sending_address VARCHAR(%s), \
			revoke_tokens_tx_property_id BIGINT, \
			revoke_tokens_tx_amount DECIMAL(%s), \
			revoke_tokens_tx_fee DECIMAL(%s) \
			)" % (self.revokeTokensTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.createCrowdsalePropertyTxTableName = "create_crowdsale_property_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			create_crowdsale_property_tx_hash DECIMAL(%s) PRIMARY KEY, \
			create_crowdsale_property_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			create_crowdsale_property_tx_time TIMESTAMP, \
			create_crowdsale_property_tx_sending_address VARCHAR(%s), \
			create_crowdsale_property_tx_property_id BIGINT, \
			create_crowdsale_property_tx_property_type SMALLINT, \
			create_crowdsale_property_tx_amount DECIMAL(%s), \
			create_crowdsale_property_tx_deadline TIMESTAMP, \
			create_crowdsale_property_tx_issuer_percent SMALLINT, \
			create_crowdsale_property_tx_bonus SMALLINT, \
			create_crowdsale_property_tx_fee DECIMAL(%s), \
			create_crowdsale_property_tx_tokens_per_unit DECIMAL(%s) \
			)" % (self.createCrowdsalePropertyTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

		self.closeCrowdsaleTxTableName = "close_crowdsale_transactions_omni"
		self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
			close_crowdsale_tx_hash DECIMAL(%s) PRIMARY KEY, \
			close_crowdsale_tx_block_hash DECIMAL(%s) references %s(block_hash) ON DELETE CASCADE, \
			close_crowdsale_tx_time TIMESTAMP, \
			close_crowdsale_tx_sending_address VARCHAR(%s), \
			close_crowdsale_tx_property_id BIGINT, \
			close_crowdsale_tx_fee DECIMAL(%s) \
			)" % (self.closeCrowdsaleTxTableName, HASH_PRECISION, HASH_PRECISION, self.blocksTableName, MAX_ADDRESS_LENGTH, 
				OUTPUT_VALUE_PRECISION))

		borderBlock = self.dbAccess.queryReturnAll("SELECT * FROM " + self.blocksTableName + " WHERE block_height=%s", (252316,))
		if len(borderBlock) == 0:
			self.dbAccess.queryNoReturnCommit("INSERT INTO " + self.blocksTableName + " (block_hash, block_height) VALUES (%s, %s)",
				(int("0000000000000000bfccb13b63c06cd5ee7075d2f5cc08b1b2f8e98365f5538e", 16), 252316))

	def drop(self):
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.closeCrowdsaleTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.createCrowdsalePropertyTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.revokeTokensTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.grantTokensTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.createManagedPropertyTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.createFixedPropertyTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.dexPurchaseTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.acceptSellForBitcoinTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.sellForBitcoinTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.sendOwnersTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.sendAllTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.simpleSendTxTableName,))
		self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.blocksTableName,))

	def addIndexes(self):
		self.dbAccess.queryNoReturnCommit("CREATE INDEX simple_send_transactions_time_index_omni ON %s(simple_send_tx_time)" % self.simpleSendTxTableName)
		self.dbAccess.queryNoReturnCommit("CREATE INDEX send_owners_transactions_time_index_omni ON %s(send_owners_tx_time)" % self.sendOwnersTxTableName)
		self.dbAccess.queryNoReturnCommit("CREATE INDEX grant_tokens_transactions_time_index_omni ON %s(grant_tokens_tx_time)" % self.grantTokensTxTableName)
		self.dbAccess.queryNoReturnCommit("CREATE INDEX revoke_tokens_transactions_time_index_omni ON %s(revoke_tokens_tx_time)" % self.revokeTokensTxTableName)
		self.dbAccess.queryNoReturnCommit("CREATE INDEX send_all_transactions_time_index_omni ON %s(send_all_tx_time)" % self.sendAllTxTableName)

	def dropIndexes(self):
		self.dbAccess.queryNoReturnCommit("DROP INDEX send_all_transactions_time_index_omni")
		self.dbAccess.queryNoReturnCommit("DROP INDEX revoke_tokens_transactions_time_index_omni")
		self.dbAccess.queryNoReturnCommit("DROP INDEX grant_tokens_transactions_time_index_omni")
		self.dbAccess.queryNoReturnCommit("DROP INDEX send_owners_transactions_time_index_omni")
		self.dbAccess.queryNoReturnCommit("DROP INDEX simple_send_transactions_time_index_omni")