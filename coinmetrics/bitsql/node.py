from coinmetrics.utils.jsonrpc import JsonRpcCaller
from coinmetrics.utils import bech32
from constants import *
from data import *
from datetime import datetime
import binascii

def outputValueToSatoshis(outputValue):
	if type(outputValue) != int:
		assert(float(outputValue) >= 0.0)
		valuePieces = outputValue.split(".")
		assert(len(valuePieces) == 2)
		fractionDigitCount = len(valuePieces[1])
		digits = int("".join(valuePieces))
		value = digits * 10**(8 - fractionDigitCount)
		naiveValue = float(outputValue) * 100000000.0
		assert(abs(value - naiveValue) <= 32)
	else:
		value = outputValue * 100000000
	assert(value < 10**OUTPUT_VALUE_PRECISION)
	assert(value >= 0)
	return value

class BitcoinNodeBase(object):

	def __init__(self, host, port, user, password):
		self.bitcoinAccess = JsonRpcCaller(host, port, user, password)

	def getBlockCount(self):
		return self.bitcoinAccess.call("getblockcount")

	def getBlock(self, height):
		blockHash = self.bitcoinAccess.call("getblockhash", [height])
		blockDict = self.bitcoinAccess.call("getblock", [blockHash])
		block = self.initBlock(blockDict)

		txDicts = self.getBlockTransactions(blockDict, block)
		txIndex = 0
		for txDict in txDicts:
			transaction = self.processTransaction(txDict, txIndex, block)
			block.addTransaction(transaction)
			txIndex += 1

		return block

	def getBlockTransactions(self, blockDict, blockData):
		hashes = []
		for txHash in blockDict["tx"]:
			if not self.excludeTransaction(txHash, blockData):
				hashes.append(txHash)
		if len(hashes) > 0:
			return self.bitcoinAccess.bulkCall(("getrawtransaction", [txHash, 1]) for txHash in hashes)
		else:
			return []

	def initBlock(self, blockDict):
		hashAsNumber = int(blockDict["hash"], base=16)
		assert(hashAsNumber < 10**HASH_PRECISION)
		chainWorkAsNumber = self.getBlockChainWork(blockDict)
		assert(chainWorkAsNumber < 10**CHAINWORK_PRECISION)
		blockTime = datetime.utcfromtimestamp(blockDict["time"])
		blockSize = int(blockDict["size"])
		difficulty = float(blockDict["difficulty"])
		height = int(blockDict["height"])
		return BitcoinBlockData(height, hashAsNumber, chainWorkAsNumber, blockTime, blockSize, difficulty)

	def getBlockChainWork(self, blockDict):
		return int(blockDict["chainwork"], base=16)

	def processTransaction(self, txDict, txIndex, blockData):
		transaction = self.initTransaction(txDict, txIndex, blockData)
		self.processInputs(transaction, txDict, txIndex)
		self.processOutputs(transaction, txDict, txIndex)
		return transaction

	def initTransaction(self, txDict, txIndex, blockData):
		txSize = self.getTransactionSize(txDict)
		txHashAsNumber = int(txDict["txid"], base=16)
		assert(txHashAsNumber < 10**HASH_PRECISION)
		coinbase = self.transactionIsCoinbase(txDict, txIndex, blockData)
		return BitcoinTransactionData(txHashAsNumber, txSize, blockData.blockTime, coinbase)

	def getTransactionSize(self, txDict):
		return int(txDict["size"])

	def transactionIsCoinbase(self, txDict, txIndex, blockData):
		for inputDict in txDict["vin"]:
			if "coinbase" in inputDict:
				return True
		return False

	def excludeTransaction(self, txHash, blockData):
		return False

	def processInputs(self, transaction, txDict, txIndex):
		if self.shouldProcessTxInputs(transaction, txDict, txIndex):
			index = 0
			for inputDict in txDict["vin"]:
				self.processInput(transaction, inputDict, index)
				index += 1

	def shouldProcessTxInputs(self, transaction, txDict, txIndex):
		return not transaction.coinbase

	def processInput(self, transaction, inputDict, inputIndex):
		inputTxHashAsNumber = int(inputDict["txid"], base=16)
		outputIndex = int(inputDict["vout"])
		transaction.addInput((inputTxHashAsNumber, outputIndex))

	def processOutputs(self, transaction, txDict, txIndex):
		outputIndex = 0
		for outputDict in txDict["vout"]:
			self.processOutput(transaction, outputDict, outputIndex)
			outputIndex += 1

	def processOutput(self, transaction, outputDict, outputIndex):
		if not outputDict["scriptPubKey"]["type"] in OUTPUT_TYPES:
			print outputDict["scriptPubKey"]["type"] 
		assert(outputDict["scriptPubKey"]["type"] in OUTPUT_TYPES)
		outputType = OUTPUT_TYPES[outputDict["scriptPubKey"]["type"]]

		if "addresses" in outputDict["scriptPubKey"]:
			addresses = outputDict["scriptPubKey"]["addresses"]
			for address in addresses:
				assert(len(address) <= MAX_ADDRESS_LENGTH)
				assert(len(address) > 0)
		else:
			addresses = []

		if outputType in [OUTPUT_TYPES["nonstandard"], OUTPUT_TYPES["nulldata"], OUTPUT_TYPES["multisig"]]:
			pass
		elif outputType == OUTPUT_TYPES["pubkey"]:
			if "addresses" in outputDict["scriptPubKey"]:
				assert(len(addresses) == 1)
		elif outputType in [OUTPUT_TYPES["witness_v0_keyhash"], OUTPUT_TYPES["witness_v0_scripthash"]]:
			assert(len(addresses) <= 1)
			if len(addresses) == 0:
				prefix = "wkh_" if outputType == OUTPUT_TYPES["witness_v0_keyhash"] else "wsh_"
				addresses = [bech32.encode(prefix, 0, [ord(s) for s in binascii.unhexlify(outputDict["scriptPubKey"]["hex"][4:])])]
		else:
			assert(len(addresses) == 1)

		scriptHex = outputDict["scriptPubKey"]["hex"]
		value = outputValueToSatoshis(outputDict["value"])
		transaction.addOutput((outputIndex, outputType, addresses, scriptHex, value))


class BitcoinNode(BitcoinNodeBase):

	def excludeTransaction(self, txHash, blockData):
		txHash = int(txHash, base=16)
		if (blockData.blockHeight == 91842 and txHash == 96714513404922958314624647138985365973445136781445592526454084119790809023897) or (blockData.blockHeight == 91880 and txHash == 103012905635419619419213554242971767587813086721766557307841598175607561106536):
			return True
		else:
			return False

# joinsplits: 3306, 3308
class ZcashNode(BitcoinNodeBase):
	
	def getTransactionSize(self, txDict):
		# Zcash node doesn't report tx size
		return 0

	def initTransaction(self, txDict, txIndex, blockData):
		data = super(ZcashNode, self).initTransaction(txDict, txIndex, blockData)
		return ZcashTransactionData(data.txHash, data.txSize, data.txTime, data.coinbase)

	def processTransaction(self, txDict, txIndex, blockData):
		transaction = super(ZcashNode, self).processTransaction(txDict, txIndex, blockData)
		for joinSplit in txDict["vjoinsplit"]:
			valueOld = outputValueToSatoshis(joinSplit["vpub_old"])
			valueNew = outputValueToSatoshis(joinSplit["vpub_new"])
			transaction.addJoinSplit((valueOld, valueNew))
		return transaction


class DogecoinNode(BitcoinNodeBase):

	def getTransactionSize(self, txDict):
		# Dogecoin node doesn't report tx size
		return 0


# zerocoin mints: 880002
# zerocoin spends: 880029
# zpiv staking: 1156138
class PivxNode(BitcoinNode):

	def transactionIsCoinbase(self, txDict, txIndex, blockData):
		isCoinbase = super(PivxNode, self).transactionIsCoinbase(txDict, txIndex, blockData)
		return isCoinbase or (blockData.blockHeight > 259200 and txIndex == 1)

	def getTransactionSize(self, txDict):
		# PIVX node doesn't report tx size
		return 0

	def initTransaction(self, txDict, txIndex, blockData):
		data = super(PivxNode, self).initTransaction(txDict, txIndex, blockData)
		return PivxTransactionData(data.txHash, data.txSize, data.txTime, data.coinbase)

	def processInput(self, transaction, inputDict, index):
		inputTxHashAsNumber = int(inputDict["txid"], base=16)
		if inputTxHashAsNumber == 0:
			amount = self.bitcoinAccess.call("getspentzerocoinamount", [transaction.getHashString(), index])
			transaction.addZerocoinSpend(outputValueToSatoshis(amount))
		else:
			return super(PivxNode, self).processInput(transaction, inputDict, index)

	def shouldProcessTxInputs(self, transaction, txDict, txIndex):
		return not (transaction.coinbase and txIndex == 0)

	def processOutput(self, transaction, outputDict, outputIndex):
		if outputDict["scriptPubKey"]["type"] == "zerocoinmint":
			transaction.addZerocoinMint(outputValueToSatoshis(outputDict["value"]))
		else:
			return super(PivxNode, self).processOutput(transaction, outputDict, outputIndex)


class VergeNode(BitcoinNodeBase):

	def getBlockChainWork(self, blockDict):
		return 0

	def getTransactionSize(self, txDict):
		return 0


class DecredNode(BitcoinNodeBase):

	def __init__(self, host, port, user, password):
		self.bitcoinAccess = JsonRpcCaller(host, port, user, password, tls=True, tlsVerify=False)

	def getBlockChainWork(self, blockDict):
		return 0

	def getTransactionSize(self, txDict):
		return 0

	def excludeTransaction(self, txHash, blockData):
		duplicateTransactions = [
			("752db9a8fa003bb7fbacad57627001973b6b95500cb0aab0dfe406483467ac10", 83822),
			("8521fb31190eacd9aaf4b27862ef88e55c6a6de8b66f241733800ddaa0b27e1b", 83912),
			("0042f3ec6660fdfb566bec6147d28e277d04c9b117d9f656f433d14b6c00167b", 83912),
			("1593791a34585557554ec85e49e39afda330dd745e7a82fc0efb0003bfae71a9", 83912),
			("d075e4a96ffb0cae0be81ad9c9e3a77ad491d1586a61a533a48460891317fb73", 83912),
			("cdea5df0ca44027336ec85c2bd3da792237fba1c582599494cda6780204ae128", 83912),
			("63b1a7dcffe67fcdd962f86f467a122ac48fa31667a71c6e7f4c138feabdb43f", 87859),
			("bf7a1a036ab4a79b6ef6062fb7e56e509660055e2a99bb346867b132d4fb5da8", 88437),
			("70f9f77fa969609c3928e024caaa872a99e07a4a6c2d05732f30b34df3935333", 88724),
			("e5b88eaaacedd86de1a670fd47ab705dbdad767df09bf0902fcbceab8987a28f", 90215),
		]
		missingInfoTransactions = [
			"f3b0e919ccac9b78b7fb15cb3e5e1f93f7dc0ed05080de3e8d67577f38b2c03d", 
			"50893c9acf2776a8e7cee7674431142cd41292370387a0b8e79bd8d577a5c55f", 
			"3d3c6581d9092db691e1624ed67a9647edab0027843f3583776a25d2dd3223e4", 
			"daa402d95ac86763e02d8e6eaac227fc66ae9a13de27d4782758f37c178b72d8", 
			"67f78257c355a86d8ebe2b0a5c053e73a26c9cdb43838085c21c1d715fda3487", 
			"5b4e88991b1a217ac2763be68da209b113c5e7d613d14cd9298a4e64d8596589", 
			"62dcaa62b47ddb6b5b1a5458b40ab2bad4160f668e2343a43130420cfd8cf3ec", 
			"2d045cf863a3f813fbe513e5a3d5b24ee447c01d43d8f6d840d2804a053b6d20", 
			"8a8a5903aa7bd8ea735e0219e216a0738858c9ae3ad4c436ef20168b9d9c1922",
			"b419240eab69242421a9183a0f5b3d6a4129c17cd0556b318dbe112df6cd866c",
			"f75dca3407378e73ef14d089dfb958a1e69578575242faffd9a02b641a547818",
			"e6171883344e3c9dbbfc544e451becd408233318c071949979d67f94d122e17f",
			"1310c7b01ce435598e30e74c4407431c27b5bdc03283a5e831f26d3a79497b4c",
			"a68d110d662d04aa3f4100894edccaddbd26252529a7d399c196f6667567e6dc",
			"69cff27b13002aa81e41598b844f45c32b1d4cb1b23ff82b53456ded49140a73",
			"48de4e70c6f556a7c33f18d7085f937855c4d3b285c9b8b16f348cb44d1d4e82",
		]
		return (txHash, blockData.blockHeight) in duplicateTransactions or txHash in missingInfoTransactions

	def getBlockTransactions(self, blockDict, blockData):
		baseTxs = super(DecredNode, self).getBlockTransactions(blockDict, blockData)
		if "stx" in blockDict:
			stakeTxs = self.bitcoinAccess.bulkCall(("getrawtransaction", [txHash, 1]) for txHash in blockDict["stx"])
			return baseTxs + stakeTxs
		else:
			return baseTxs

	def initTransaction(self, txDict, txIndex, blockData):
		data = super(DecredNode, self).initTransaction(txDict, txIndex, blockData)
		return DecredTransactionData(data.txHash, data.txSize, data.txTime, data.coinbase)

	def processTransaction(self, txDict, txIndex, blockData):
		result = super(DecredNode, self).processTransaction(txDict, txIndex, blockData)
		assert(not (result.vote and result.ticket))
		return result

	def processInput(self, transaction, inputDict, index):
		if "stakebase" in inputDict:
			transaction.vote = True
			if "txid" in inputDict:
				return super(DecredNode, self).processInput(transaction, inputDict, index)
		else:
			return super(DecredNode, self).processInput(transaction, inputDict, index)

	def processOutput(self, transaction, outputDict, outputIndex):
		outputType = outputDict["scriptPubKey"]["type"]
		if outputType == "sstxcommitment" or outputType == "stakerevoke" or outputType == "stakesubmission":
			transaction.ticket = True
		return super(DecredNode, self).processOutput(transaction, outputDict, outputIndex)

