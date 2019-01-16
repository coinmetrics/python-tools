from coinmetrics.bitsql.constants import *


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

    def getCoinbaseScriptsTableName(self):
        return self.coinbaseScriptsTableName

    def init(self):
        self.blocksTableName = self.asset + "_blocks"
        self.transactionsTableName = self.asset + "_transactions"
        self.outputsTableName = self.asset + "_outputs"
        self.coinbaseScriptsTableName = self.asset + "_coinbase_scripts"

        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            block_hash DECIMAL(%s) PRIMARY KEY, \
            block_height INTEGER, \
            block_size INTEGER, \
            block_time TIMESTAMP, \
            block_median_time TIMESTAMP, \
            block_difficulty DOUBLE PRECISION, \
            block_chainwork DECIMAL(%s) \
            )" % (self.blocksTableName, HASH_PRECISION, CHAINWORK_PRECISION))

        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            tx_hash DECIMAL(%s) PRIMARY KEY, \
            tx_block_hash DECIMAL(%s), \
            tx_size INTEGER, \
            tx_time TIMESTAMP, \
            tx_median_time TIMESTAMP, \
            tx_coinbase BOOLEAN \
            )" % (self.transactionsTableName, HASH_PRECISION, HASH_PRECISION))

        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            output_tx_hash DECIMAL(%s), \
            output_index INTEGER, \
            output_type SMALLINT, \
            output_addresses VARCHAR(%s)[], \
            output_script BYTEA, \
            output_spend_signature BYTEA, \
            output_value_satoshi DECIMAL(%s), \
            output_spending_tx_hash DECIMAL(%s), \
            output_time_created TIMESTAMP, \
            output_time_spent TIMESTAMP, \
            output_median_time_created TIMESTAMP, \
            output_median_time_spent TIMESTAMP, \
            PRIMARY KEY(output_tx_hash, output_index)\
            )" % (self.outputsTableName, HASH_PRECISION, MAX_ADDRESS_LENGTH, OUTPUT_VALUE_PRECISION, HASH_PRECISION))

        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            coinbase_script_tx_hash DECIMAL(%s) PRIMARY KEY, \
            coinbase_script_hex BYTEA \
            )" % (self.coinbaseScriptsTableName, HASH_PRECISION))

    def drop(self):
        self.dropIndexes()
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.coinbaseScriptsTableName,))
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.outputsTableName,))
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.transactionsTableName,))
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.blocksTableName,))

    def addIndexes(self):
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_block_time_index ON %s_blocks(block_time)" % (self.asset, self.asset))
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_block_height_index ON %s_blocks(block_height)" % (self.asset, self.asset))
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_tx_time_index ON %s_transactions(tx_time)" % (self.asset, self.asset))
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_output_time_spent_index ON %s_outputs(output_time_spent)" % (self.asset, self.asset))
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_output_time_created_index ON %s_outputs(output_time_created)" % (self.asset, self.asset))

    def dropIndexes(self):
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_output_time_created_index" % (self.asset,))
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_output_time_spent_index" % (self.asset,))
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_tx_time_index" % (self.asset,))
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_block_height_index" % (self.asset,))
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_block_time_index" % (self.asset,))

    def vacuum(self):
        isolationLevel = self.dbAccess.connection.isolation_level
        self.dbAccess.connection.set_isolation_level(0)
        self.dbAccess.queryNoReturnNoCommit("VACUUM ANALYZE %s" % self.getOutputsTableName())
        self.dbAccess.connection.set_isolation_level(isolationLevel)


class ZcashSchema(BitcoinSchema):

    def getJoinSplitsTableName(self):
        return self.joinSplitsTableName

    def getSaplingPaymentTableName(self):
        return self.saplingPaymentTableName

    def init(self):
        super(ZcashSchema, self).init()
        self.joinSplitsTableName = self.asset + "_joinsplits"
        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            id SERIAL PRIMARY KEY, \
            joinsplit_tx_hash DECIMAL(%s), \
            joinsplit_value_old DECIMAL(%s), \
            joinsplit_value_new DECIMAL(%s), \
            joinsplit_time TIMESTAMP \
            )" % (self.joinSplitsTableName, HASH_PRECISION, OUTPUT_VALUE_PRECISION, OUTPUT_VALUE_PRECISION))

        self.saplingPaymentTableName = self.asset + "_sapling_payments"
        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            sapling_payment_tx_hash DECIMAL(%s), \
            sapling_payment_value_balance DECIMAL(%s), \
            sapling_payment_input_count INTEGER, \
            sapling_payment_output_count INTEGER, \
            sapling_payment_time TIMESTAMP, \
            PRIMARY KEY(sapling_payment_tx_hash) \
        )" % (self.saplingPaymentTableName, HASH_PRECISION, OUTPUT_VALUE_PRECISION))

    def drop(self):
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.saplingPaymentTableName,))
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.joinSplitsTableName,))
        super(ZcashSchema, self).drop()

    def addIndexes(self):
        super(ZcashSchema, self).addIndexes()
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_joinsplit_time_index ON %s_joinsplits(joinsplit_time)" % (self.asset, self.asset))
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_sapling_payment_time_index ON %s_sapling_payments(sapling_payment_time)" % (self.asset, self.asset))

    def dropIndexes(self):
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_sapling_payment_time_index" % (self.asset,))
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_joinsplit_time_index" % (self.asset,))
        super(ZcashSchema, self).dropIndexes()


class PivxSchema(BitcoinSchema):

    def getZerocoinMintsTableName(self):
        return self.zerocoinMintsTableName

    def getZerocoinSpendsTableName(self):
        return self.zerocoinSpendsTableName

    def init(self):
        super(PivxSchema, self).init()
        self.zerocoinMintsTableName = self.asset + "_zerocoin_mints"
        self.zerocoinSpendsTableName = self.asset + "_zerocoin_spends"

        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            id SERIAL PRIMARY KEY, \
            zerocoin_mint_tx_hash DECIMAL(%s), \
            zerocoin_mint_value DECIMAL(%s), \
            zerocoin_mint_time TIMESTAMP \
            )" % (self.zerocoinMintsTableName, HASH_PRECISION, OUTPUT_VALUE_PRECISION))

        self.dbAccess.queryNoReturnCommit("CREATE TABLE IF NOT EXISTS %s (\
            id SERIAL PRIMARY KEY, \
            zerocoin_spend_tx_hash DECIMAL(%s), \
            zerocoin_spend_value DECIMAL(%s), \
            zerocoin_spend_time TIMESTAMP \
            )" % (self.zerocoinSpendsTableName, HASH_PRECISION, OUTPUT_VALUE_PRECISION))

    def drop(self):
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.zerocoinSpendsTableName,))
        self.dbAccess.queryNoReturnCommit("DROP TABLE IF EXISTS %s" % (self.zerocoinMintsTableName,))
        super(PivxSchema, self).drop()

    def addIndexes(self):
        super(PivxSchema, self).addIndexes()
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_zerocoin_mint_time_index ON %s_zerocoin_mints(zerocoin_mint_time)" % (self.asset, self.asset))
        self.dbAccess.queryNoReturnCommit("CREATE INDEX %s_zerocoin_spend_time_index ON %s_zerocoin_spends(zerocoin_spend_time)" % (self.asset, self.asset))

    def dropIndexes(self):
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_zerocoin_spend_time_index" % (self.asset,))
        self.dbAccess.queryNoReturnCommit("DROP INDEX IF EXISTS %s_zerocoin_mint_time_index" % (self.asset,))
        super(PivxSchema, self).dropIndexes()


class DecredSchema(BitcoinSchema):

    def init(self):
        super(DecredSchema, self).init()
        self.dbAccess.queryNoReturnCommit("ALTER TABLE %s ADD COLUMN IF NOT EXISTS tx_vote BOOLEAN" % self.transactionsTableName)
        self.dbAccess.queryNoReturnCommit("ALTER TABLE %s ADD COLUMN IF NOT EXISTS tx_ticket BOOLEAN" % self.transactionsTableName)
