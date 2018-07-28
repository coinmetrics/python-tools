class OmniQuery(object):

	def __init__(self, dbAccess, schema):
		self.dbAccess = dbAccess
		self.schema = schema

	def getBlockHeight(self):
		result = self.dbAccess.queryReturnOne("SELECT max(block_height) FROM %s" % self.schema.getBlocksTableName())[0]
		return result if result is not None else 0


class PropertyQuery(object):

	def __init__(self, dbAccess, schema, propertyId, assetName):
		self.dbAccess = dbAccess
		self.schema = schema
		self.propertyId = propertyId
		self.assetName = assetName

	def getAsset(self):
		return self.assetName

	def run(self, text):
		return self.dbAccess.queryReturnAll(text)

	def getTxCountBetween(self, minTime, maxTime):
		result = self.dbAccess.queryReturnOne("WITH \
			txs AS (\
				SELECT simple_send_tx_hash FROM " + self.schema.getSimpleSendTxTableName() + " \
					WHERE simple_send_tx_property_id=%s AND simple_send_tx_time >= %s AND simple_send_tx_time < %s \
				UNION ALL \
				SELECT send_owners_tx_hash FROM " + self.schema.getSendOwnersTxTableName() + "\
					WHERE send_owners_tx_property_id=%s AND send_owners_tx_time >= %s AND send_owners_tx_time < %s \
				UNION ALL \
				SELECT send_all_tx_hash FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s AND send_all_tx_time >= %s AND send_all_tx_time < %s GROUP BY send_all_tx_hash \
			) \
			SELECT COUNT(*) FROM txs", (self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime,
				self.propertyId, minTime, maxTime))
		return result[0] if result[0] is not None else 0

	def getOutputVolumeBetween(self, minTime, maxTime):
		result = self.dbAccess.queryReturnOne("WITH \
			txs AS (\
				SELECT simple_send_tx_amount AS amount FROM " + self.schema.getSimpleSendTxTableName() + " \
					WHERE simple_send_tx_property_id=%s AND simple_send_tx_time >= %s AND simple_send_tx_time < %s \
				UNION ALL \
				SELECT send_owners_tx_amount AS amount FROM " + self.schema.getSendOwnersTxTableName() + "\
					WHERE send_owners_tx_property_id=%s AND send_owners_tx_time >= %s AND send_owners_tx_time < %s \
				UNION ALL \
				SELECT send_all_tx_amount AS amount FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s AND send_all_tx_time >= %s AND send_all_tx_time < %s \
			) \
			SELECT SUM(amount) FROM txs", (self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime, 
				self.propertyId, minTime, maxTime))
		return result[0] if result[0] is not None else 0

	def getMedianTransactionValueBetween(self, minTime, maxTime):
		result = self.dbAccess.queryReturnOne("WITH \
			txs AS (\
				SELECT simple_send_tx_amount AS amount FROM " + self.schema.getSimpleSendTxTableName() + " \
					WHERE simple_send_tx_property_id=%s AND simple_send_tx_time >= %s AND simple_send_tx_time < %s \
				UNION ALL \
				SELECT send_owners_tx_amount AS amount FROM " + self.schema.getSendOwnersTxTableName() + "\
					WHERE send_owners_tx_property_id=%s AND send_owners_tx_time >= %s AND send_owners_tx_time < %s \
				UNION ALL \
				SELECT SUM(send_all_tx_amount) AS amount FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s AND send_all_tx_time >= %s AND send_all_tx_time < %s GROUP BY send_all_tx_hash \
			) \
			SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FROM txs", 
				(self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime))
		return result[0]	

	def getActiveAddressesCountBetween(self, minTime, maxTime):
		result = self.dbAccess.queryReturnOne("WITH \
			addresses AS (\
				SELECT simple_send_tx_sending_address AS address FROM " + self.schema.getSimpleSendTxTableName() + " \
					WHERE simple_send_tx_property_id=%s AND simple_send_tx_time >= %s AND simple_send_tx_time < %s \
				UNION ALL \
				SELECT simple_send_tx_receiving_address AS address FROM " + self.schema.getSimpleSendTxTableName() + " \
					WHERE simple_send_tx_property_id=%s AND simple_send_tx_time >= %s AND simple_send_tx_time < %s \
				UNION ALL \
				SELECT send_owners_tx_sending_address AS address FROM " + self.schema.getSendOwnersTxTableName() + "\
					WHERE send_owners_tx_property_id=%s AND send_owners_tx_time >= %s AND send_owners_tx_time < %s \
				UNION ALL \
				SELECT send_all_tx_sending_address AS address FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s AND send_all_tx_time >= %s AND send_all_tx_time < %s \
				UNION ALL \
				SELECT send_all_tx_receiving_address AS address FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s AND send_all_tx_time >= %s AND send_all_tx_time < %s \
			) \
			SELECT COUNT(DISTINCT address) FROM addresses", 
				(self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime,
					self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime))
		return result[0] if result[0] is not None else 0


class ManagedPropertyQuery(PropertyQuery):

	def getMinBlockTime(self):
		result = self.dbAccess.queryReturnOne("SELECT MIN(create_managed_property_tx_time) FROM " \
			+ self.schema.getCreateManagedPropertyTxTableName() + " WHERE create_managed_property_tx_property_id=%s", (self.propertyId,))
		return result[0]

	def getMaxBlockTime(self):
		return self.dbAccess.queryReturnOne("WITH \
			max_times AS (\
				SELECT MAX(simple_send_tx_time) AS value FROM " + self.schema.getSimpleSendTxTableName() +  "\
					WHERE simple_send_tx_property_id=%s \
				UNION ALL \
				SELECT MAX(grant_tokens_tx_time) AS value FROM " + self.schema.getGrantTokensTxTableName() + "\
					WHERE grant_tokens_tx_property_id=%s \
				UNION ALL \
				SELECT MAX(revoke_tokens_tx_time) AS value FROM " + self.schema.getRevokeTokensTxTableName() + "\
					WHERE revoke_tokens_tx_property_id=%s \
				UNION ALL \
				SELECT MAX(send_owners_tx_time) AS value FROM " + self.schema.getSendOwnersTxTableName() + "\
					WHERE send_owners_tx_property_id=%s \
				UNION ALL \
				SELECT MAX(send_all_tx_time) AS value FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s \
			) \
			SELECT MAX(value) FROM max_times", (self.propertyId, self.propertyId, self.propertyId, self.propertyId, self.propertyId))[0]	

	def getRewardBetween(self, minTime, maxTime):
		result = self.dbAccess.queryReturnOne("WITH \
			txs AS (\
				SELECT grant_tokens_tx_amount AS amount FROM " + self.schema.getGrantTokensTxTableName() + " \
					WHERE grant_tokens_tx_property_id=%s AND grant_tokens_tx_time >= %s AND grant_tokens_tx_time < %s \
				UNION ALL \
				SELECT -revoke_tokens_tx_amount AS amount FROM " + self.schema.getRevokeTokensTxTableName() + "\
					WHERE revoke_tokens_tx_property_id=%s AND revoke_tokens_tx_time >= %s AND revoke_tokens_tx_time < %s \
			) \
			SELECT SUM(amount) FROM txs", (self.propertyId, minTime, maxTime, self.propertyId, minTime, maxTime))
		return result[0] if result[0] is not None else 0				


class CrowdsalePropertyQuery(PropertyQuery):

	def getMinBlockTime(self):
		result = self.dbAccess.queryReturnOne("SELECT MIN(create_crowdsale_property_tx_time) FROM " \
			+ self.schema.getCreateCrowdsalePropertyTxTableName() + " WHERE create_crowdsale_property_tx_property_id=%s", (self.propertyId,))
		return result[0]

	def getMaxBlockTime(self):
		return self.dbAccess.queryReturnOne("WITH \
			max_times AS (\
				SELECT MAX(simple_send_tx_time) AS value FROM " + self.schema.getSimpleSendTxTableName() +  "\
					WHERE simple_send_tx_property_id=%s \
				UNION ALL \
				SELECT MAX(send_owners_tx_time) AS value FROM " + self.schema.getSendOwnersTxTableName() + "\
					WHERE send_owners_tx_property_id=%s \
				UNION ALL \
				SELECT MAX(send_all_tx_time) AS value FROM " + self.schema.getSendAllTxTableName() + "\
					WHERE send_all_tx_property_id=%s \
			) \
			SELECT MAX(value) FROM max_times", (self.propertyId, self.propertyId, self.propertyId))[0]	


class TetherQuery(ManagedPropertyQuery):

	def __init__(self, dbAccess, schema):
		super(TetherQuery, self).__init__(dbAccess, schema, 31, "usdt")


class MaidSafeCoinQuery(CrowdsalePropertyQuery):

	def __init__(self, dbAccess, schema):
		super(MaidSafeCoinQuery, self).__init__(dbAccess, schema, 3, "maid")