from coinmetrics.utils.jsonrpc import JsonRpcCaller
from coinmetrics.bitsql.omni import *
from coinmetrics.bitsql.constants import HASH_PRECISION, MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION
from coinmetrics.bitsql.node import outputValueToSatoshis
from datetime import datetime

SELL_FOR_BITCOIN_ACTIONS = {
	"new": 1,
	"update": 2,
	"cancel": 3,
}

PROPERTY_TYPES = {
	"indivisible": 1,
	"divisible": 2,
}

IGNORE_TX_TYPES = set([25, 26, 28, 70, 71, 185, 65534])

def omniOutputValueSatoshi(amount):
	if amount.find(".") == -1:
		amount = int(amount) * 100000000
		assert(amount < 10**OUTPUT_VALUE_PRECISION)
		return amount
	else:
		return outputValueToSatoshis(amount)

class OmniNode(object):

	def __init__(self, host, port, user, password):
		self.omniAccess = JsonRpcCaller(host, port, user, password)
		self.txProcessors = {
			0: self.processSimpleSend,
			3: self.processSendOwners,
			4: self.processSendAll,
			20: self.processSellForBitcoin,
			22: self.processAcceptSellForBitcoin,
			50: self.processCreateFixedProperty,
			51: self.processCreateCrowdsaleProperty,
			53: self.processCloseCrowdsale,
			54: self.processCreateManagedProperty,
			55: self.processGrantTokens,
			56: self.processRevokeTokens,
		}

	def getBlockCount(self):
		return self.omniAccess.call("getblockcount")

	def getBlock(self, height):
		blockHash = self.omniAccess.call("getblockhash", [height])
		blockDict = self.omniAccess.call("getblock", [blockHash])
		hashAsNumber = int(blockDict["hash"], base=16)
		assert(hashAsNumber < 10**HASH_PRECISION)
		block = OmniBlockData(height, hashAsNumber, datetime.utcfromtimestamp(blockDict["time"]))

		transactionHashes = self.omniAccess.call("omni_listblocktransactions", [height])
		# no bulk request due to Omni node freezing
		for txHash in transactionHashes:
			txInfo = self.omniAccess.call("omni_gettransaction", [txHash])
			if not "valid" in txInfo:
				assert(txInfo["type"] == "DEx Purchase")
				self.processDexPurchase(txInfo, block)
			elif txInfo["valid"]:
				txType = txInfo["type_int"]
				if txType in self.txProcessors:
					self.txProcessors[txType](txInfo, block)
				elif txType not in IGNORE_TX_TYPES:
					print txInfo
					print "unknown tx type: {0}".format(txType)
					assert(False)
				

		return block

	def processSimpleSend(self, txInfo, block):
		assert(len(txInfo["referenceaddress"]) < MAX_ADDRESS_LENGTH)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniSimpleSendTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			txInfo["referenceaddress"], omniOutputValueSatoshi(txInfo["amount"]), fee)
		block.addSimpleSendTransaction(txData)

	def processSendAll(self, txInfo, block):
		assert(len(txInfo["referenceaddress"]) < MAX_ADDRESS_LENGTH)
		assert(len(txInfo["sendingaddress"]) < MAX_ADDRESS_LENGTH)
		hashAsNumber = int(txInfo["txid"], base=16)
		assert(hashAsNumber < 10**HASH_PRECISION)
		txTime = datetime.utcfromtimestamp(txInfo["blocktime"])
		for index, send in enumerate(txInfo["subsends"]):
			txData = OmniSendAllTransaction(block.blockHash, hashAsNumber, index, txTime, send["propertyid"], 
				txInfo["sendingaddress"], txInfo["referenceaddress"], omniOutputValueSatoshi(send["amount"]),
				omniOutputValueSatoshi(txInfo["fee"]))
			block.addSendAllTransaction(txData)

	def processSendOwners(self, txInfo, block):
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniSendOwnersTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			omniOutputValueSatoshi(txInfo["amount"]), fee)
		block.addSendOwnersTransaction(txData)

	def processSellForBitcoin(self, txInfo, block):
		if not txInfo["action"] in SELL_FOR_BITCOIN_ACTIONS:
			print txInfo
			print "unknown action of sell-for-bitcoin tx: {0}".format(txInfo["action"])
			assert(False)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniSellForBitcoinTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			omniOutputValueSatoshi(txInfo["amount"]), fee, omniOutputValueSatoshi(txInfo["feerequired"]),
			omniOutputValueSatoshi(txInfo["bitcoindesired"]), SELL_FOR_BITCOIN_ACTIONS[txInfo["action"]])
		block.addSellForBitcoinTransaction(txData)

	def processAcceptSellForBitcoin(self, txInfo, block):
		assert(len(txInfo["referenceaddress"]) < MAX_ADDRESS_LENGTH)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniAcceptSellForBitcoinTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			omniOutputValueSatoshi(txInfo["amount"]), fee, txInfo["referenceaddress"])
		block.addAcceptSellForBitcoinTransaction(txData)

	def processCreateFixedProperty(self, txInfo, block):
		if not txInfo["propertytype"] in PROPERTY_TYPES:
			print txInfo
			print "unknown property type: {0}".format(txInfo["propertytype"])
			assert(False)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniCreateFixedPropertyTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			omniOutputValueSatoshi(txInfo["amount"]), fee, PROPERTY_TYPES[txInfo["propertytype"]])
		block.addCreateFixedPropertyTransaction(txData)

	def processCreateManagedProperty(self, txInfo, block):
		if not txInfo["propertytype"] in PROPERTY_TYPES:
			print txInfo
			print "unknown property type: {0}".format(txInfo["propertytype"])
			assert(False)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniCreateManagedPropertyTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			fee, PROPERTY_TYPES[txInfo["propertytype"]])
		block.addCreateManagedPropertyTransaction(txData)

	def processGrantTokens(self, txInfo, block):
		assert(len(txInfo["referenceaddress"]) < MAX_ADDRESS_LENGTH)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniGrantTokensTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			txInfo["referenceaddress"], omniOutputValueSatoshi(txInfo["amount"]), fee)
		block.addGrantTokensTransaction(txData)

	def processRevokeTokens(self, txInfo, block):
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniRevokeTokensTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, 
			omniOutputValueSatoshi(txInfo["amount"]), fee)
		block.addRevokeTokensTransaction(txData)

	def processCreateCrowdsaleProperty(self, txInfo, block):
		if not txInfo["propertytype"] in PROPERTY_TYPES:
			print txInfo
			print "unknown property type: {0}".format(txInfo["propertytype"])
			assert(False)
		try:
			deadline = datetime.utcfromtimestamp(txInfo["deadline"])
		except ValueError:
			deadline = datetime(2100, 1, 1)
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniCreateCrowdsalePropertyTransaction(block.blockHash, txHash, txTime, propertyId, sendingAddress, omniOutputValueSatoshi(txInfo["amount"]), 
			fee, PROPERTY_TYPES[txInfo["propertytype"]], omniOutputValueSatoshi(txInfo["tokensperunit"]), 
			deadline, txInfo["earlybonus"], txInfo["percenttoissuer"])
		block.addCreateCrowdsalePropertyTransaction(txData)

	def processCloseCrowdsale(self, txInfo, block):
		txHash, txTime, sendingAddress, propertyId, fee = self.getCommonTxAttributes(txInfo)
		txData = OmniTransactionBase(block.blockHash, txHash, txTime, propertyId, sendingAddress, fee)
		block.addCloseCrowdsaleTransaction(txData)		

	def processDexPurchase(self, txInfo, block):
		assert(len(txInfo["sendingaddress"]) < MAX_ADDRESS_LENGTH)
		hashAsNumber = int(txInfo["txid"], base=16)
		assert(hashAsNumber < 10**HASH_PRECISION)
		txTime = datetime.utcfromtimestamp(txInfo["blocktime"])
		for index, purchase in enumerate(txInfo["purchases"]):
			if purchase["valid"]:
				assert(len(purchase["referenceaddress"]) < MAX_ADDRESS_LENGTH)
				txData = OmniDexPurchaseTransaction(block.blockHash, hashAsNumber, index, txTime, purchase["propertyid"], 
					txInfo["sendingaddress"], purchase["referenceaddress"], omniOutputValueSatoshi(purchase["amountbought"]), 
					omniOutputValueSatoshi(purchase["amountpaid"]))
				block.addDexPurchaseTransaction(txData)

	def getCommonTxAttributes(self, txInfo):
		assert(len(txInfo["sendingaddress"]) < MAX_ADDRESS_LENGTH)
		hashAsNumber = int(txInfo["txid"], base=16)
		assert(hashAsNumber < 10**HASH_PRECISION)
		txTime = datetime.utcfromtimestamp(txInfo["blocktime"])
		return hashAsNumber, txTime, txInfo["sendingaddress"], txInfo["propertyid"], omniOutputValueSatoshi(txInfo["fee"])

	
		