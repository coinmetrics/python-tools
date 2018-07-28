class OmniExporter(object):

	def __init__(self, dbAccess, schema):
		self.dbAccess = dbAccess
		self.schema = schema

	def pushBlock(self, blockData):
		self.dbAccess.queryNoReturnNoCommit("INSERT INTO " + self.schema.getBlocksTableName() + "\
		 	(block_hash, block_height) \
		 	VALUES (%s, %s)", (blockData.blockHash, blockData.blockHeight))

		self.txsInBlock = 0
		self.insertTransactions(blockData.simpleSendTransactions, self.schema.getSimpleSendTxTableName(), self.schema.getSimpleSendTxPrefix())
		self.insertTransactions(blockData.sendOwnersTransactions, self.schema.getSendOwnersTxTableName(), self.schema.getSendOwnersTxPrefix())
		self.insertTransactions(blockData.sellForBitcoinTransactions, self.schema.getSellForBitcoinTxTableName(), self.schema.getSellForBitcoinTxPrefix())
		self.insertTransactions(blockData.acceptSellForBitcoinTransactions, self.schema.getAcceptSellForBitcoinTxTableName(), self.schema.getAcceptSellForBitcoinTxPrefix())
		self.insertTransactions(blockData.dexPurchaseTransactions, self.schema.getDexPurchaseTxTableName(), self.schema.getDexPurchaseTxPrefix())
		self.insertTransactions(blockData.createFixedPropertyTransactions, self.schema.getCreateFixedPropertyTxTableName(), self.schema.getCreateFixedPropertyTxPrefix())
		self.insertTransactions(blockData.createCrowdsalePropertyTransactions, self.schema.getCreateCrowdsalePropertyTxTableName(), self.schema.getCreateCrowdsalePropertyTxPrefix())
		self.insertTransactions(blockData.createManagedPropertyTransactions, self.schema.getCreateManagedPropertyTxTableName(), self.schema.getCreateManagedPropertyTxPrefix())
		self.insertTransactions(blockData.closeCrowdsaleTransactions, self.schema.getCloseCrowdsaleTxTableName(), self.schema.getCloseCrowdsaleTxPrefix())
		self.insertTransactions(blockData.grantTokensTransactions, self.schema.getGrantTokensTxTableName(), self.schema.getGrantTokensTxPrefix())
		self.insertTransactions(blockData.revokeTokensTransactions, self.schema.getRevokeTokensTxTableName(), self.schema.getRevokeTokensTxPrefix())
		self.insertTransactions(blockData.sendAllTransactions, self.schema.getSendAllTxTableName(), self.schema.getSendAllTxPrefix())
		print "exported {0} txs".format(self.txsInBlock)

		self.dbAccess.commit()

	def insertTransactions(self, txList, tableName, prefix):
		if len(txList) == 0:
			return

		self.txsInBlock += len(txList)

		keys, _ = txList[0].getAttributes(prefix)
		keysString = "(" + ", ".join([key for key in keys]) + ")"

		tuples = []
		for tx in txList:
			_, values = tx.getAttributes(prefix)
			tuples.append(values)

		self.dbAccess.executeValues("INSERT INTO " + tableName + " " + keysString + " VALUES %s", tuples, 512)
