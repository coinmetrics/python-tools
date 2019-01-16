class BitcoinBlockData(object):

    def __init__(self, height, hashAsNumber, chainworkAsNumber, blockTime, blockMedianTime, blockSize, difficulty):
        self.transactions = []
        self.blockHeight = height
        self.hashAsNumber = hashAsNumber
        self.chainworkAsNumber = chainworkAsNumber
        self.blockTime = blockTime
        self.blockMedianTime = blockMedianTime
        self.blockSize = blockSize
        self.difficulty = difficulty

    def addTransaction(self, transaction):
        self.transactions.append(transaction)

    def getTransactions(self):
        return self.transactions

    def __repr__(self):
        header = "block height: %d, time: %s, tx count %d" % (self.blockHeight, self.blockTime, len(self.transactions))
        transactions = []
        for tx in self.transactions:
            transactions.append(str(tx))
        return "\n".join([header] + transactions)


class BitcoinTransactionData(object):

    def __init__(self, txHash, txSize, txTime, txMedianTime, coinbase):
        self.txHash = txHash
        self.txSize = txSize
        self.txTime = txTime
        self.txMedianTime = txMedianTime
        self.coinbase = coinbase
        self.coinbaseScript = None
        self.inputs = []
        self.outputs = []

    def getInputs(self):
        return self.inputs

    def getOutputs(self):
        return self.outputs

    def addInput(self, inputData):
        self.inputs.append(inputData)

    def addOutput(self, outputData):
        self.outputs.append(outputData)

    def setCoinbaseScript(self, coinbaseScript):
        self.coinbaseScript = coinbaseScript

    def getCoinbaseScript(self):
        return self.coinbaseScript

    def getHashString(self):
        result = hex(self.txHash)[2:]
        while len(result) < 64:
            result = "0" + result
        return result

    def __repr__(self):
        header = "tx %s %s\n" % (self.txHash, "coinbase" if self.coinbase else "")
        inputsRepr = "\n".join(["inputs:"] + [str(i) for i in self.inputs])
        outputsRepr = "\n".join(["\noutputs:"] + [str(o) for o in self.outputs])
        return header + inputsRepr + outputsRepr


class ZcashTransactionData(BitcoinTransactionData):

    def __init__(self, txHash, txSize, txTime, txMedianTime, coinbase):
        super(ZcashTransactionData, self).__init__(txHash, txSize, txTime, txMedianTime, coinbase)
        self.joinSplits = []
        self.saplingPayments = []

    def getJoinSplits(self):
        return self.joinSplits

    def getSaplingPayments(self):
        return self.saplingPayments

    def addJoinSplit(self, joinSplitData):
        self.joinSplits.append(joinSplitData)

    def addSaplingPayment(self, saplingPayment):
        self.saplingPayments.append(saplingPayment)


class PivxTransactionData(BitcoinTransactionData):

    def __init__(self, txHash, txSize, txTime, txMedianTime, coinbase):
        super(PivxTransactionData, self).__init__(txHash, txSize, txTime, txMedianTime, coinbase)
        self.zerocoinMints = []
        self.zerocoinSpends = []

    def getZerocoinMints(self):
        return self.zerocoinMints

    def getZerocoinSpends(self):
        return self.zerocoinSpends

    def addZerocoinMint(self, mintedValue):
        self.zerocoinMints.append(mintedValue)

    def addZerocoinSpend(self, spentValue):
        self.zerocoinSpends.append(spentValue)

    def __repr__(self):
        base = super(PivxTransactionData, self).__repr__()
        mintsRepr = "\n".join(["\nzc mints:"] + [str(i) for i in self.zerocoinMints])
        spendsRepr = "\n".join(["\nzc spends:"] + [str(o) for o in self.zerocoinSpends])
        return base + mintsRepr + spendsRepr


class DecredTransactionData(BitcoinTransactionData):

    def __init__(self, txHash, txSize, txTime, txMedianTime, coinbase):
        super(DecredTransactionData, self).__init__(txHash, txSize, txTime, txMedianTime, coinbase)
        self.vote = False
        self.ticket = False

    def __repr__(self):
        base = super(DecredTransactionData, self).__repr__()
        if self.vote:
            base += "\nvote"
        if self.ticket:
            base += "\nticket"
        return base
