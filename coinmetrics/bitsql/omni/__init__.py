class OmniBlockData(object):

    def __init__(self, blockHeight, blockHash, blockTime):
        self.blockHeight = blockHeight
        self.blockHash = blockHash
        self.blockTime = blockTime
        self.simpleSendTransactions = []
        self.sendOwnersTransactions = []
        self.sellForBitcoinTransactions = []
        self.acceptSellForBitcoinTransactions = []
        self.dexPurchaseTransactions = []
        self.createFixedPropertyTransactions = []
        self.createCrowdsalePropertyTransactions = []
        self.closeCrowdsaleTransactions = []
        self.createManagedPropertyTransactions = []
        self.grantTokensTransactions = []
        self.revokeTokensTransactions = []
        self.sendAllTransactions = []

    def addSimpleSendTransaction(self, tx):
        self.simpleSendTransactions.append(tx)

    def addSendOwnersTransaction(self, tx):
        self.sendOwnersTransactions.append(tx)

    def addSellForBitcoinTransaction(self, tx):
        self.sellForBitcoinTransactions.append(tx)

    def addAcceptSellForBitcoinTransaction(self, tx):
        self.acceptSellForBitcoinTransactions.append(tx)

    def addDexPurchaseTransaction(self, tx):
        self.dexPurchaseTransactions.append(tx)

    def addCreateFixedPropertyTransaction(self, tx):
        self.createFixedPropertyTransactions.append(tx)

    def addCreateCrowdsalePropertyTransaction(self, tx):
        self.createCrowdsalePropertyTransactions.append(tx)

    def addCloseCrowdsaleTransaction(self, tx):
        self.closeCrowdsaleTransactions.append(tx)

    def addCreateManagedPropertyTransaction(self, tx):
        self.createManagedPropertyTransactions.append(tx)

    def addGrantTokensTransaction(self, tx):
        self.grantTokensTransactions.append(tx)

    def addRevokeTokensTransaction(self, tx):
        self.revokeTokensTransactions.append(tx)

    def addSendAllTransaction(self, tx):
        self.sendAllTransactions.append(tx)


class OmniTransactionBase(object):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, fee):
        self.block_hash = txBlockHash
        self.hash = txHash
        self.time = txTime
        self.property_id = propertyId
        self.sending_address = sendingAddress
        self.fee = fee

    def getAttributes(self, prefix):
        keys = []
        values = []
        intermediate = []
        for key, value in self.__dict__.items():
            intermediate.append((prefix + "_" + key, value))
        for key, value in sorted(intermediate, key=lambda pair: pair[0]):
            keys.append(key)
            values.append(value)
        return keys, values


class OmniSimpleSendTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, receivingAddress, amount, fee):
        super(OmniSimpleSendTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.receiving_address = receivingAddress
        self.amount = amount


class OmniSendAllTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txIndex, txTime, propertyId, sendingAddress, receivingAddress, amount, fee):
        super(OmniSendAllTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.index = txIndex
        self.receiving_address = receivingAddress
        self.amount = amount


class OmniSendOwnersTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, amount, fee):
        super(OmniSendOwnersTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.amount = amount


class OmniSellForBitcoinTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, amount, fee, feeRequired, bitcoinDesired, action):
        super(OmniSellForBitcoinTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.amount = amount
        self.fee_required = feeRequired
        self.bitcoin_desired = bitcoinDesired
        self.action = action


class OmniAcceptSellForBitcoinTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, amount, fee, receivingAddress):
        super(OmniAcceptSellForBitcoinTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.amount = amount
        self.receiving_address = receivingAddress


class OmniDexPurchaseTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txIndex, txTime, propertyId, sendingAddress, receivingAddress, amountBought, amountPaid):
        self.block_hash = txBlockHash
        self.hash = txHash
        self.index = txIndex
        self.time = txTime
        self.property_id = propertyId
        self.sending_address = sendingAddress
        self.receiving_address = receivingAddress
        self.amount_bought = amountBought
        self.amount_paid = amountPaid


class OmniCreateFixedPropertyTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, amount, fee, propertyType):
        super(OmniCreateFixedPropertyTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.amount = amount
        self.property_type = propertyType   


class OmniCreateManagedPropertyTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, fee, propertyType):
        super(OmniCreateManagedPropertyTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.property_type = propertyType   


class OmniGrantTokensTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, receivingAddress, amount, fee):
        super(OmniGrantTokensTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.receiving_address = receivingAddress
        self.amount = amount


class OmniRevokeTokensTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, amount, fee):
        super(OmniRevokeTokensTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.amount = amount


class OmniCreateCrowdsalePropertyTransaction(OmniTransactionBase):

    def __init__(self, txBlockHash, txHash, txTime, propertyId, sendingAddress, amount, fee, propertyType, tokensPerUnit, deadline, bonus, issuerPercent):
        super(OmniCreateCrowdsalePropertyTransaction, self).__init__(txBlockHash, txHash, txTime, propertyId, sendingAddress, fee)
        self.amount = amount
        self.property_type = propertyType   
        self.deadline = deadline
        self.tokens_per_unit = tokensPerUnit
        self.bonus = bonus
        self.issuer_percent = issuerPercent