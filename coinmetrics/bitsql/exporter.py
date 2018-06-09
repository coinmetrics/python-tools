from datetime import datetime

class BulkExporterBase(object):

	def pushBlock(self, blockData):
		self.prologue(blockData)
		self.insertBlock(blockData)
		self.insertTransactions(blockData)
		self.insertOutputs(blockData)
		self.insertInputs(blockData)
		self.additionalProcessing(blockData)
		self.epilogue(blockData)

	def prologue(self, blockData):
		pass

	def epilogue(self, blockData):
		pass

	def additionalProcessing(self, blockData):
		pass


class BitcoinExporter(BulkExporterBase):

	def __init__(self, dbAccess, schema):
		self.asset = schema.getAsset()
		self.schema = schema
		self.dbAccess = dbAccess

	def prologue(self, blockData):
		inputsCount, outputsCount = 0, 0
		for tx in blockData.getTransactions():
			inputsCount += len(tx.getInputs())
			outputsCount += len(tx.getOutputs())
		print "txs: %d, inputs: %d, outputs: %d" % (len(blockData.getTransactions()), inputsCount, outputsCount)

	def epilogue(self, blockData):
		self.dbAccess.commit()

	def insertBlock(self, blockData):
		self.dbAccess.queryNoReturnNoCommit("INSERT INTO " + self.schema.getBlocksTableName() + "\
		 	(block_hash, block_height, block_size, block_time, block_difficulty, block_chainwork) \
		 	VALUES (%s, %s, %s, %s, %s, %s)", (blockData.hashAsNumber, blockData.blockHeight, 
		 		blockData.blockSize, blockData.blockTime, blockData.difficulty, blockData.chainworkAsNumber))

	def insertTransactions(self, blockData):
		transactionTuples = []
		for tx in blockData.getTransactions():
			transactionTuples.append((tx.txHash, tx.txSize, tx.coinbase, blockData.hashAsNumber, tx.txTime))
		self.dbAccess.executeValues("INSERT INTO " + self.schema.getTransactionsTableName() + "\
		 	(tx_hash, tx_size, tx_coinbase, tx_block_hash, tx_time) VALUES %s", transactionTuples, 512)

	def insertInputs(self, blockData):
		batchUpdateData = []
		for tx in blockData.getTransactions():
			for inputTxHash, outputIndex in tx.getInputs():
				batchUpdateData.append((inputTxHash, outputIndex, tx.txHash, tx.txTime))

		self.dbAccess.executeValues("UPDATE " + self.schema.getOutputsTableName() + "\
		 	SET output_spending_tx_hash=data.output_spending_tx_hash, output_time_spent=data.output_time_spent \
		 	FROM (VALUES %s) AS data (output_tx_hash, output_index, output_spending_tx_hash, output_time_spent) \
		 	WHERE " + self.schema.getOutputsTableName() + ".output_tx_hash=data.output_tx_hash AND " 
		 	+ self.schema.getOutputsTableName() + ".output_index=data.output_index", batchUpdateData, 512)

	def insertOutputs(self, blockData):
		rows = []
		for tx in blockData.getTransactions():
			for outputIndex, outputType, addresses, scriptHex, value in tx.getOutputs():
				rows.append((tx.txHash, outputIndex, outputType, addresses, 
					bytearray.fromhex(scriptHex), value, tx.txTime, None))
		self.dbAccess.executeValues("INSERT INTO " + self.schema.getOutputsTableName() + "\
		 	(output_tx_hash, output_index, output_type, output_addresses, output_script, \
		 	output_value_satoshi, output_time_created, output_time_spent) VALUES %s", rows, 512)


class ZcashExporter(BitcoinExporter):

	def additionalProcessing(self, blockData):
		for tx in blockData.getTransactions():
			for valueOld, valueNew in tx.getJoinSplits():
				self.dbAccess.queryNoReturnNoCommit("INSERT INTO " + self.schema.getJoinSplitsTableName() + "\
				 	(joinsplit_tx_hash, joinsplit_value_old, joinsplit_value_new, joinsplit_time) \
				 	VALUES (%s, %s, %s, %s)", (tx.txHash, valueOld, valueNew, tx.txTime))


class PivxExporter(BitcoinExporter):

	def additionalProcessing(self, blockData):
		for tx in blockData.getTransactions():
			for mintValue in tx.getZerocoinMints():
				self.dbAccess.queryNoReturnNoCommit("INSERT INTO " + self.schema.getZerocoinMintsTableName() + "\
				 	(zerocoin_mint_tx_hash, zerocoin_mint_value, zerocoin_mint_time) \
				 	VALUES (%s, %s, %s)", (tx.txHash, mintValue, tx.txTime))
			for spendValue in tx.getZerocoinSpends():
				self.dbAccess.queryNoReturnNoCommit("INSERT INTO " + self.schema.getZerocoinSpendsTableName() + "\
				 	(zerocoin_spend_tx_hash, zerocoin_spend_value, zerocoin_spend_time) \
				 	VALUES (%s, %s, %s)", (tx.txHash, spendValue, tx.txTime))


class DecredExporter(BitcoinExporter):

	def insertTransactions(self, blockData):
		transactionTuples = []
		for tx in blockData.getTransactions():
			transactionTuples.append((tx.txHash, tx.txSize, tx.coinbase, blockData.hashAsNumber, tx.txTime,
			 	tx.vote, tx.ticket))
		self.dbAccess.executeValues(
			"INSERT INTO " + self.schema.getTransactionsTableName() + "\
			 (tx_hash, tx_size, tx_coinbase, tx_block_hash, tx_time, tx_vote, tx_ticket) VALUES %s", 
			 transactionTuples, 512)