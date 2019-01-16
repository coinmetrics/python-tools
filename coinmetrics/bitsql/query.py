from datetime import timedelta
from dateutil.relativedelta import relativedelta


class BitcoinQuery(object):

    def __init__(self, dbAccess, schema):
        self.dbAccess = dbAccess
        self.schema = schema
        self.asset = schema.getAsset()
        self.blocksTable = schema.getBlocksTableName()
        self.txTable = schema.getTransactionsTableName()
        self.outputsTable = schema.getOutputsTableName()

    def getSchema(self):
        return self.schema

    def getAsset(self):
        return self.schema.getAsset()

    def getBlockHeight(self):
        result = self.dbAccess.queryReturnOne("SELECT max(block_height) FROM %s" % self.schema.getBlocksTableName())[0]
        return result

    def getBlockTime(self, height):
        result = self.dbAccess.queryReturnOne("SELECT block_time FROM %s WHERE block_height=%d" % (self.schema.getBlocksTableName(), height))
        return result[0]

    def getMinBlockTime(self):
        result = self.dbAccess.queryReturnOne("SELECT min(block_time) FROM %s" % self.schema.getBlocksTableName())
        return result[0] if result is not None else 0

    def getMaxBlockTime(self):
        result = self.dbAccess.queryReturnOne("SELECT max(block_time) FROM %s" % self.schema.getBlocksTableName())
        return result[0] if result is not None else 0

    def getBlockHeightsBefore(self, maxTime):
        result = self.dbAccess.queryReturnAll("SELECT block_height FROM " + self.blocksTable + " WHERE block_time <= %s", (maxTime,))
        return [row[0] for row in result]

    def getTransactionsBetween(self, minDate, maxDate):
        return self.dbAccess.queryReturnAll("\
            SELECT \
                * \
            FROM " + self.txTable + " WHERE \
                (tx_time >= %s AND tx_time < %s)", (minDate, maxDate))

    def getInputsBetween(self, minDate, maxDate):
        return self.dbAccess.queryReturnAll("\
            SELECT \
                * \
            FROM " + self.outputsTable + " WHERE \
                (output_time_spent >= %s AND output_time_spent < %s)", (minDate, maxDate))

    def getInputTxHashAndAddressesBetween(self, minDate, maxDate):
        return self.dbAccess.queryReturnAll("\
            SELECT \
                output_spending_tx_hash, output_addresses \
            FROM " + self.outputsTable + " WHERE \
                (output_time_spent >= %s AND output_time_spent < %s)", (minDate, maxDate))

    def getAverageDifficultyBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                AVG(block_difficulty) \
            FROM " + self.blocksTable + " WHERE \
                block_time >= %s AND block_time < %s", (minDate, maxDate))
        return result[0]

    def getBlockSizeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                SUM(block_size) \
            FROM " + self.blocksTable + " WHERE \
                block_time >= %s AND block_time < %s", (minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getBlockCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                COUNT(*) \
            FROM " + self.blocksTable + " WHERE \
                block_time >= %s AND block_time < %s", (minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getTxCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                COUNT(*) \
            FROM " + self.txTable + " WHERE \
                tx_coinbase=false AND (tx_time >= %s AND tx_time < %s)", (minDate, maxDate))
        return result[0]

    def getOutputVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) AND output_type>1), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false) \
            SELECT \
                sum(o.output_value_satoshi) \
            FROM o JOIN t ON \
                o.output_tx_hash = t.tx_hash \
            LEFT JOIN (\
                SELECT \
                    o.output_tx_hash as change_output_tx_hash, \
                    o.output_index as change_output_index \
                FROM o JOIN i ON \
                    (o.output_tx_hash = i.output_spending_tx_hash) AND \
                    ((o.output_addresses && i.output_addresses) = true)) change ON \
                o.output_tx_hash=change.change_output_tx_hash AND o.output_index=change.change_output_index \
            WHERE change.change_output_tx_hash is NULL", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getHeuristicalOutputVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) \
                    AND output_type>1 \
                    AND ((output_time_spent is NULL) OR \
                         (EXTRACT(EPOCH FROM (output_time_spent - output_time_created)) > 2400))), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false) \
            SELECT \
                sum(o.output_value_satoshi) \
            FROM o JOIN t ON \
                o.output_tx_hash = t.tx_hash \
            LEFT JOIN (\
                SELECT \
                    o.output_tx_hash as change_output_tx_hash, \
                    o.output_index as change_output_index \
                FROM o JOIN i ON \
                    (o.output_tx_hash = i.output_spending_tx_hash) AND \
                    ((o.output_addresses && i.output_addresses) = true)) change ON \
                o.output_tx_hash=change.change_output_tx_hash AND o.output_index=change.change_output_index \
            WHERE change.change_output_tx_hash is NULL", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getActiveAddressesCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                count(distinct address) \
            FROM ( \
                SELECT \
                    unnest(output_addresses) AS address \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) \
                UNION ALL \
                SELECT \
                    unnest(output_addresses) AS address \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)) active_addresses", (minDate, maxDate, minDate, maxDate))
        return result[0]

    def getFeesVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            volume_o AS (SELECT sum(o.output_value_satoshi) v FROM o JOIN t ON t.tx_hash=o.output_tx_hash), \
            volume_i AS (SELECT sum(i.output_value_satoshi) v FROM i JOIN t ON t.tx_hash=i.output_spending_tx_hash) \
            SELECT \
                coalesce(volume_i.v, 0) - coalesce(volume_o.v, 0) \
            FROM \
                volume_o CROSS JOIN volume_i", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getMedianFeeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    coalesce(sum(o.output_value_satoshi), 0) as sum_outputs, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash), \
            si AS (\
                SELECT \
                    coalesce(sum(i.output_value_satoshi), 0) as sum_inputs, \
                    t.tx_hash \
                FROM t JOIN i ON \
                    t.tx_hash=i.output_spending_tx_hash \
                GROUP BY t.tx_hash), \
            fees AS (\
                SELECT \
                    coalesce(si.sum_inputs, 0) - coalesce(so.sum_outputs, 0) as fee, \
                    si.tx_hash as hash \
                FROM si FULL OUTER JOIN so ON \
                    si.tx_hash=so.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY fee) FROM fees", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getMedianTransactionValueBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    sum(o.output_value_satoshi) as sum_outputs \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                LEFT JOIN (\
                    SELECT \
                        o.output_tx_hash as change_output_tx_hash, \
                        o.output_index as change_output_index \
                    FROM o JOIN i ON \
                        (o.output_tx_hash = i.output_spending_tx_hash) AND \
                        ((o.output_addresses && i.output_addresses) = true)) change ON \
                    o.output_tx_hash=change.change_output_tx_hash AND o.output_index=change.change_output_index \
                WHERE change.change_output_tx_hash is NULL \
                GROUP BY t.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum_outputs) FROM so", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getPaymentCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) AND output_type>1), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    GREATEST(count(*) - 1, 0) as payments, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash) \
            SELECT sum(payments) FROM so", (minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getRewardBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    tx_coinbase=true AND (tx_time >= %s AND tx_time < %s)), \
            o AS (\
                SELECT \
                    output_value_satoshi, \
                    output_tx_hash \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)) \
            SELECT sum(o.output_value_satoshi) FROM t JOIN o ON t.tx_hash=o.output_tx_hash", (minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getTotalSupplyBetween(self, minDate, maxDate):
        previousValue = self.dbAccess.queryReturnOne("""
            SELECT
                value
            FROM
                statistic_total_supply_{asset}
            WHERE
                date = %s
        """.format(
            asset=self.asset
        ), (minDate - timedelta(days=1),))
        previousValue = previousValue[0] if previousValue is not None else 0

        todayReward = self.getRewardBetween(minDate, maxDate)
        todayFees = self.getFeesVolumeBetween(minDate, maxDate)
        return previousValue + todayReward - todayFees

    def get30DNaiveCirculatingSupplyBetween(self, _, maxDate):
        delta = timedelta(days=30)

        result = self.dbAccess.queryReturnOne("""
            SELECT 
                SUM(output_value_satoshi)
            FROM 
                {outputs}
            JOIN
                {transactions}
            ON
                output_tx_hash = tx_hash
            WHERE
                tx_coinbase IS FALSE
            AND
                ((output_time_spent is NULL) OR (output_time_spent >= %s))
            AND
                ((output_time_created >= %s) AND (output_time_created < %s))
        """.format(
            outputs=self.outputsTable, transactions=self.txTable,
        ), (maxDate, maxDate - delta, maxDate,))
        return result[0]

    # We use an iterative approach to compute this stat.
    # Today's circulating supply is equal to:
    #     yesterday's circulating supply
    #   + non-reward unspent outputs created today
    #   - minus spent outputs created that are no more than ${maxDate - delta} old
    #   - minus outputs unspent to this day but created exacly minDate - delta days ago
    def _getCirculatingSupplyBetween(self, minDate, maxDate, delta, deltaString):
        previousValue = self.dbAccess.queryReturnOne("""
            SELECT
                value
            FROM
                statistic_{deltaString}_circulating_supply_{asset}
            WHERE
                date = %s
        """.format(
            deltaString=deltaString,
            asset=self.asset
        ), (minDate - timedelta(days=1),))
        previousValue = previousValue[0] if previousValue is not None else 0

        spentValue = self.dbAccess.queryReturnOne("""
            SELECT
                COALESCE(SUM(output_value_satoshi), 0)
            FROM
                {outputs}
            JOIN
                {transactions}
            ON
                output_tx_hash = tx_hash
            WHERE
                tx_coinbase IS FALSE
            AND
                (output_time_created >= %s AND output_time_created < %s)
            AND
                (output_time_spent >= %s AND output_time_spent < %s)
        """.format(
            outputs=self.outputsTable, transactions=self.txTable,
        ), (minDate - delta, minDate, minDate, maxDate))
        spentValue = spentValue[0] if spentValue is not None else 0

        maturedValue = self.dbAccess.queryReturnOne("""
            SELECT
                COALESCE(SUM(output_value_satoshi), 0)
            FROM
                {outputs}
            JOIN
                {transactions}
            ON
                output_tx_hash = tx_hash
            WHERE
                tx_coinbase IS FALSE
            AND
                (output_time_created >= %s AND output_time_created < %s)
            AND
                (output_time_spent IS NULL OR output_time_spent >= %s)
        """.format(
            outputs=self.outputsTable, transactions=self.txTable,
        ), (minDate - delta, maxDate - delta, maxDate))
        maturedValue = maturedValue[0] if maturedValue is not None else 0

        createdValue = self.dbAccess.queryReturnOne("""
            SELECT
                COALESCE(SUM(output_value_satoshi), 0)
            FROM
                {outputs}
            JOIN
                {transactions}
            ON
                output_tx_hash = tx_hash
            WHERE
                tx_coinbase IS FALSE
            AND
                (output_time_created >= %s AND output_time_created < %s)
            AND
                ((output_time_spent is NULL) OR (output_time_spent >= %s))
        """.format(
            outputs=self.outputsTable, transactions=self.txTable,
        ), (minDate, maxDate, maxDate))
        createdValue = createdValue[0] if createdValue is not None else 0

        return previousValue - spentValue - maturedValue + createdValue

    def get1YCirculatingSupplyBetween(self, minDate, maxDate):
        return self._getCirculatingSupplyBetween(minDate, maxDate, timedelta(days=365), "1y")

    def get180DCirculatingSupplyBetween(self, minDate, maxDate):
        return self._getCirculatingSupplyBetween(minDate, maxDate, timedelta(days=180), "180d")

    def get30DCirculatingSupplyBetween(self, minDate, maxDate):
        return self._getCirculatingSupplyBetween(minDate, maxDate, timedelta(days=30), "30d")

    def run(self, text):
        return self.dbAccess.queryReturnAll(text)


class ZcashQuery(BitcoinQuery):
    
    def __init__(self, dbAccess, schema):
        super(ZcashQuery, self).__init__(dbAccess, schema)
        self.joinSplitsTable = self.schema.getJoinSplitsTableName()
        self.saplingPaymentTable = self.schema.getSaplingPaymentTableName()

    def getFeesVolumeBetween(self, minDate, maxDate):
        baseFees = super(ZcashQuery, self).getFeesVolumeBetween(minDate, maxDate)
        joinSplitDiffs = self.getJoinSplitsDiffValueBetween(minDate, maxDate)
        saplingValueBalance = self.getSaplingValueBalanceBetween(minDate, maxDate)
        return baseFees + joinSplitDiffs + saplingValueBalance

    def getOutputVolumeBetween(self, minDate, maxDate):
        baseResult = super(ZcashQuery, self).getOutputVolumeBetween(minDate, maxDate)
        joinSplitResult = self.getJoinSplitsNegativeDiffValueBetween(minDate, maxDate)
        saplingValueBalance = self.getSaplingNegativeValueBalanceBetween(minDate, maxDate)
        return baseResult + joinSplitResult + saplingValueBalance

    def getHeuristicalOutputVolumeBetween(self, minDate, maxDate):
        baseResult = super(ZcashQuery, self).getHeuristicalOutputVolumeBetween(minDate, maxDate)
        joinSplitResult = self.getJoinSplitsNegativeDiffValueBetween(minDate, maxDate)
        saplingValueBalance = self.getSaplingNegativeValueBalanceBetween(minDate, maxDate)
        return baseResult + joinSplitResult + saplingValueBalance

    def getJoinSplitsDiffValueBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                sum(joinsplit_value_new) - sum(joinsplit_value_old) AS value \
            FROM " + self.joinSplitsTable + " WHERE \
                (joinsplit_time >= %s AND joinsplit_time < %s)", (minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getJoinSplitsNegativeDiffValueBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                sum(-least( joinsplit_value_new - joinsplit_value_old, 0)) AS value \
            FROM " + self.joinSplitsTable + " WHERE \
                (joinsplit_time >= %s AND joinsplit_time < %s)", (minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getSaplingValueBalanceBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                sum(sapling_payment_value_balance) AS value \
            FROM " + self.saplingPaymentTable + " WHERE \
                (sapling_payment_time >= %s AND sapling_payment_time < %s)", (minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getSaplingNegativeValueBalanceBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                sum(-least(sapling_payment_value_balance, 0)) AS value \
            FROM " + self.saplingPaymentTable + " WHERE \
                (sapling_payment_time >= %s AND sapling_payment_time < %s)", (minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getMedianFeeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            joinsplit AS (\
                SELECT \
                    joinsplit_value_new - joinsplit_value_old as value, \
                    joinsplit_tx_hash \
                FROM " + self.joinSplitsTable  + " WHERE \
                    (joinsplit_time >= %s AND joinsplit_time < %s)), \
            sapling_payment AS (\
                SELECT \
                    sapling_payment_value_balance as value, \
                    sapling_payment_tx_hash \
                FROM " + self.saplingPaymentTable + " WHERE \
                    (sapling_payment_time >= %s AND sapling_payment_time < %s)), \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    -coalesce(sum(o.output_value_satoshi), 0) as sum, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    coalesce(sum(i.output_value_satoshi), 0) as sum, \
                    t.tx_hash \
                FROM t JOIN i ON \
                    t.tx_hash=i.output_spending_tx_hash \
                GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    coalesce(sum(joinsplit.value), 0) AS sum, \
                    t.tx_hash \
                FROM t JOIN joinsplit ON \
                    t.tx_hash=joinsplit.joinsplit_tx_hash \
                GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    coalesce(sum(sapling_payment.value), 0) AS sum, \
                    t.tx_hash \
                FROM t JOIN sapling_payment ON \
                    t.tx_hash=sapling_payment.sapling_payment_tx_hash \
                GROUP BY t.tx_hash), \
            fees AS (\
                SELECT \
                    coalesce(sum(so.sum), 0) as fee \
                FROM so \
                GROUP BY so.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY fee) FROM fees", (minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getMedianTransactionValueBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            joinsplit AS (\
                SELECT \
                    -least(joinsplit_value_new - joinsplit_value_old, 0) as value, \
                    joinsplit_tx_hash \
                FROM " + self.joinSplitsTable  + " WHERE \
                    (joinsplit_time >= %s AND joinsplit_time < %s)), \
            sapling_payment AS (\
                SELECT \
                    -least(sapling_payment_value_balance, 0) as value, \
                    sapling_payment_tx_hash \
                FROM " + self.saplingPaymentTable + " WHERE \
                    (sapling_payment_time >= %s AND sapling_payment_time < %s)), \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                 FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    coalesce(sum(o.output_value_satoshi), 0) as partial_sum, \
                    t.tx_hash as tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                WHERE (o.output_tx_hash, o.output_index) NOT IN (\
                    SELECT \
                        o.output_tx_hash as output_tx_hash, \
                        o.output_index as output_index \
                    FROM o JOIN i ON \
                        (o.output_tx_hash = i.output_spending_tx_hash) \
                        AND \
                        ((o.output_addresses && i.output_addresses) = true)) \
                    GROUP BY t.tx_hash), \
            sj AS (\
                SELECT \
                    coalesce(sum(joinsplit.value), 0) as partial_sum, \
                    t.tx_hash as tx_hash \
                FROM t JOIN joinsplit ON \
                    t.tx_hash=joinsplit.joinsplit_tx_hash \
                GROUP BY t.tx_hash), \
            ssp AS (\
                SELECT \
                    coalesce(sum(sapling_payment.value), 0) as partial_sum, \
                    t.tx_hash as tx_hash \
                FROM t JOIN sapling_payment ON \
                    t.tx_hash=sapling_payment.sapling_payment_tx_hash \
                GROUP BY t.tx_hash), \
            sall AS (\
                SELECT * FROM sj UNION ALL \
                SELECT * FROM so UNION ALL \
                SELECT * FROM ssp \
            ), \
            total AS (SELECT sum(partial_sum) AS sum_total, tx_hash FROM sall GROUP BY tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum_total) FROM total", (minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getPaymentCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) AND output_type>1), \
            sapling_payment AS (\
                SELECT \
                    sapling_payment_output_count as output_count, \
                    sapling_payment_tx_hash \
                FROM " + self.saplingPaymentTable + " WHERE \
                    (sapling_payment_time >= %s AND sapling_payment_time < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            all_outputs AS (\
                SELECT \
                    count(*) as output_count, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    sum(sapling_payment.output_count) as output_count, \
                    t.tx_hash \
                FROM t JOIN sapling_payment ON \
                    t.tx_hash=sapling_payment.sapling_payment_tx_hash \
                GROUP BY t.tx_hash), \
            total AS (SELECT greatest(sum(output_count) - 1, 0) as payments FROM all_outputs GROUP BY tx_hash) \
            SELECT sum(payments) FROM total", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0


class PivxQuery(BitcoinQuery):

    def __init__(self, dbAccess, schema):
        super(PivxQuery, self).__init__(dbAccess, schema)
        self.zerocoinMintsTableName = self.schema.getZerocoinMintsTableName()
        self.zerocoinSpendsTableName = self.schema.getZerocoinSpendsTableName()

    def getRewardBetween(self, minDate, maxDate):
        baseResult = super(PivxQuery, self).getRewardBetween(minDate, maxDate)
        coinbaseInputs = self.getCoinbaseInputVolumeBetween(minDate, maxDate)
        coinbaseZerocoinMints = self.getZerocoinMintsVolumeBetween(minDate, maxDate, True)
        coinbaseZerocoinSpends = self.getZerocoinSpendsVolumeBetween(minDate, maxDate, True)
        return baseResult - coinbaseInputs + coinbaseZerocoinMints - coinbaseZerocoinSpends

    def getOutputVolumeBetween(self, minDate, maxDate):
        baseResult = super(PivxQuery, self).getOutputVolumeBetween(minDate, maxDate)
        zerocoinMints = self.getZerocoinMintsVolumeBetween(minDate, maxDate, False)
        return baseResult + zerocoinMints

    def getHeuristicalOutputVolumeBetween(self, minDate, maxDate):
        baseResult = super(PivxQuery, self).getHeuristicalOutputVolumeBetween(minDate, maxDate)
        zerocoinMints = self.getZerocoinMintsVolumeBetween(minDate, maxDate, False)
        return baseResult + zerocoinMints

    def getFeesVolumeBetween(self, minDate, maxDate):
        baseFees = super(PivxQuery, self).getFeesVolumeBetween(minDate, maxDate)
        zerocoinMints = self.getZerocoinMintsVolumeBetween(minDate, maxDate, False)
        zerocoinSpends = self.getZerocoinSpendsVolumeBetween(minDate, maxDate, False)
        return baseFees - zerocoinMints + zerocoinSpends

    def getZerocoinMintsVolumeBetween(self, minDate, maxDate, coinbase):
        result = self.dbAccess.queryReturnOne("WITH \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=%s), \
            z AS (\
                SELECT \
                    zerocoin_mint_tx_hash, \
                    zerocoin_mint_value \
                FROM " + self.zerocoinMintsTableName + " WHERE \
                    (zerocoin_mint_time >= %s AND zerocoin_mint_time < %s)) \
            SELECT \
                sum(zerocoin_mint_value) \
            FROM z JOIN t ON \
                z.zerocoin_mint_tx_hash=t.tx_hash", (minDate, maxDate, coinbase, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getZerocoinSpendsVolumeBetween(self, minDate, maxDate, coinbase):
        result = self.dbAccess.queryReturnOne("WITH \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=%s), \
            z AS (\
                SELECT \
                    zerocoin_spend_tx_hash, \
                    zerocoin_spend_value \
                FROM " + self.zerocoinSpendsTableName + " WHERE \
                    (zerocoin_spend_time >= %s AND zerocoin_spend_time < %s)) \
            SELECT \
                sum(zerocoin_spend_value) \
            FROM z JOIN t ON \
                z.zerocoin_spend_tx_hash=t.tx_hash", (minDate, maxDate, coinbase, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getMedianFeeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            zspend AS (\
                SELECT \
                    zerocoin_spend_tx_hash, \
                    zerocoin_spend_value \
                FROM " + self.zerocoinSpendsTableName + " WHERE \
                    (zerocoin_spend_time >= %s AND zerocoin_spend_time < %s)), \
            zmint AS (\
                SELECT \
                    zerocoin_mint_tx_hash, \
                    zerocoin_mint_value \
                FROM " + self.zerocoinMintsTableName + " WHERE \
                    (zerocoin_mint_time >= %s AND zerocoin_mint_time < %s)), \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    -coalesce(sum(o.output_value_satoshi), 0) as sum, \
                    t.tx_hash \
                FROM t JOIN o ON t.tx_hash=o.output_tx_hash GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    coalesce(sum(i.output_value_satoshi), 0) as sum, \
                    t.tx_hash \
                FROM t JOIN i ON t.tx_hash=i.output_spending_tx_hash GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    coalesce(sum(zspend.zerocoin_spend_value), 0) as sum, \
                    t.tx_hash \
                FROM t join zspend ON t.tx_hash=zspend.zerocoin_spend_tx_hash GROUP BY t.tx_hash \
                UNION ALL \
                SELECT \
                    -coalesce(sum(zmint.zerocoin_mint_value), 0) as sum, \
                    t.tx_hash \
                FROM t join zmint ON t.tx_hash=zmint.zerocoin_mint_tx_hash GROUP BY t.tx_hash), \
            fees AS (\
                SELECT \
                    coalesce(sum(so.sum), 0) as fee \
                FROM so \
                GROUP BY so.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY fee) FROM fees", (minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getMedianTransactionValueBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            z AS (\
                SELECT \
                    zerocoin_mint_tx_hash, \
                    zerocoin_mint_value \
                FROM " + self.zerocoinMintsTableName + " WHERE \
                    (zerocoin_mint_time >= %s AND zerocoin_mint_time < %s)), \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    coalesce(sum(o.output_value_satoshi), 0) as sum_outputs, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                WHERE \
                    (o.output_tx_hash, o.output_index) NOT IN \
                        (SELECT \
                            o.output_tx_hash as output_tx_hash, \
                            o.output_index as output_index \
                        FROM o JOIN i ON \
                            (o.output_tx_hash = i.output_spending_tx_hash) AND \
                            ((o.output_addresses && i.output_addresses) = true)) \
                GROUP BY t.tx_hash), \
            sz AS (\
                SELECT \
                    sum(z.zerocoin_mint_value) as sum_zerocoin, \
                    t.tx_hash \
                FROM t JOIN z ON \
                    t.tx_hash=z.zerocoin_mint_tx_hash \
                GROUP BY t.tx_hash), \
            total AS (\
                SELECT \
                    (coalesce(so.sum_outputs, 0) + coalesce(sz.sum_zerocoin, 0)) as sum_total \
                FROM so FULL OUTER JOIN sz ON \
                    so.tx_hash=sz.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum_total) FROM total", (minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getPaymentCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            z AS (\
                SELECT \
                    zerocoin_mint_tx_hash \
                FROM " + self.zerocoinMintsTableName + " WHERE \
                    (zerocoin_mint_time >= %s AND zerocoin_mint_time < %s)), \
            o AS (\
                SELECT \
                    output_tx_hash \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) AND output_type>1), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false), \
            so AS (\
                SELECT \
                    count(*) as payments, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash), \
            sz AS (\
                SELECT \
                    count(*) as payments, \
                    t.tx_hash \
                FROM t JOIN z ON \
                    t.tx_hash=z.zerocoin_mint_tx_hash \
                GROUP BY t.tx_hash), \
            total AS (\
                SELECT \
                    greatest(coalesce(so.payments, 0) + coalesce(sz.payments, 0) - 1, 0) AS payments \
                FROM so FULL OUTER JOIN sz ON \
                    so.tx_hash=sz.tx_hash) \
            SELECT sum(payments) FROM total", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getCoinbaseInputVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    tx_coinbase=true AND (tx_time >= %s AND tx_time < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)) \
            SELECT \
                sum(output_value_satoshi) \
            FROM i JOIN t ON \
                t.tx_hash=i.output_spending_tx_hash", (minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0


class DecredQuery(BitcoinQuery):

    def getTxCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("\
            SELECT \
                COUNT(*) \
            FROM " + self.txTable + " WHERE \
                tx_coinbase=false AND tx_vote=false AND (tx_time >= %s AND tx_time < %s)", (minDate, maxDate))
        return result[0]

    def getFeesVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false AND tx_vote=false), \
            volume_o AS (SELECT sum(o.output_value_satoshi) v FROM o JOIN t ON t.tx_hash=o.output_tx_hash), \
            volume_i AS (SELECT sum(i.output_value_satoshi) v FROM i JOIN t ON t.tx_hash=i.output_spending_tx_hash) \
            SELECT \
                coalesce(volume_i.v, 0) - coalesce(volume_o.v, 0) \
            FROM \
                volume_o CROSS JOIN volume_i", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getOutputVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) AND output_type>1), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false AND tx_vote=false AND tx_ticket=false) \
            SELECT \
                sum(o.output_value_satoshi) \
            FROM o JOIN t ON \
                o.output_tx_hash = t.tx_hash \
            LEFT JOIN (\
                SELECT \
                    o.output_tx_hash as change_output_tx_hash, \
                    o.output_index as change_output_index \
                FROM o JOIN i ON \
                    (o.output_tx_hash = i.output_spending_tx_hash) AND \
                    ((o.output_addresses && i.output_addresses) = true)) change ON \
                o.output_tx_hash=change.change_output_tx_hash AND o.output_index=change.change_output_index \
            WHERE change.change_output_tx_hash is NULL", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getHeuristicalOutputVolumeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) \
                    AND output_type>1 \
                    AND ((output_time_spent is NULL) OR \
                         (EXTRACT(EPOCH FROM (output_time_spent - output_time_created)) > 2400))), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false AND tx_vote=false AND tx_ticket=false) \
            SELECT \
                sum(o.output_value_satoshi) \
            FROM o JOIN t ON \
                o.output_tx_hash = t.tx_hash \
            LEFT JOIN (\
                SELECT \
                    o.output_tx_hash as change_output_tx_hash, \
                    o.output_index as change_output_index \
                FROM o JOIN i ON \
                    (o.output_tx_hash = i.output_spending_tx_hash) AND \
                    ((o.output_addresses && i.output_addresses) = true)) change ON \
                o.output_tx_hash=change.change_output_tx_hash AND o.output_index=change.change_output_index \
            WHERE change.change_output_tx_hash is NULL", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getMedianFeeBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false AND tx_vote=false), \
            so AS (\
                SELECT \
                    coalesce(sum(o.output_value_satoshi), 0) as sum_outputs, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash), \
            si AS (\
                SELECT \
                    coalesce(sum(i.output_value_satoshi), 0) as sum_inputs, \
                    t.tx_hash \
                FROM t JOIN i ON \
                    t.tx_hash=i.output_spending_tx_hash \
                GROUP BY t.tx_hash), \
            fees AS (\
                SELECT \
                    coalesce(si.sum_inputs, 0) - coalesce(so.sum_outputs, 0) as fee, \
                    si.tx_hash as hash \
                FROM si FULL OUTER JOIN so ON \
                    si.tx_hash=so.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY fee) FROM fees", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getMedianTransactionValueBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_index, \
                    output_value_satoshi, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            i AS (\
                SELECT \
                    output_spending_tx_hash, \
                    output_index, \
                    output_addresses \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false AND tx_vote=false AND tx_ticket=false), \
            so AS (\
                SELECT \
                    sum(o.output_value_satoshi) as sum_outputs \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                LEFT JOIN (\
                    SELECT \
                        o.output_tx_hash as change_output_tx_hash, \
                        o.output_index as change_output_index \
                    FROM o JOIN i ON \
                        (o.output_tx_hash = i.output_spending_tx_hash) AND \
                        ((o.output_addresses && i.output_addresses) = true)) change ON \
                    o.output_tx_hash=change.change_output_tx_hash AND o.output_index=change.change_output_index \
                WHERE change.change_output_tx_hash is NULL \
                GROUP BY t.tx_hash) \
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum_outputs) FROM so", (minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]

    def getPaymentCountBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            o AS (\
                SELECT \
                    output_tx_hash, \
                    output_value_satoshi \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s) AND output_type>1), \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    (tx_time >= %s AND tx_time < %s) AND tx_coinbase=false AND tx_ticket=false AND tx_vote=false), \
            so AS (\
                SELECT \
                    GREATEST(count(*) - 1, 0) as payments, \
                    t.tx_hash \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash \
                GROUP BY t.tx_hash) \
            SELECT sum(payments) FROM so", (minDate, maxDate, minDate, maxDate))
        return result[0] if result[0] is not None else 0

    def getRewardBetween(self, minDate, maxDate):
        result = self.dbAccess.queryReturnOne("WITH \
            t AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    tx_coinbase=true AND (tx_time >= %s AND tx_time < %s)), \
            o AS (\
                SELECT \
                    output_value_satoshi, \
                    output_tx_hash \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            pow AS (\
                SELECT \
                    sum(o.output_value_satoshi) AS value \
                FROM t JOIN o ON \
                    t.tx_hash=o.output_tx_hash), \
            st AS (\
                SELECT \
                    tx_hash \
                FROM " + self.txTable + " WHERE \
                    tx_vote=true AND (tx_time >= %s AND tx_time < %s)), \
            so AS (\
                SELECT \
                    output_value_satoshi, \
                    output_tx_hash \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_created >= %s AND output_time_created < %s)), \
            si AS (\
                SELECT \
                    output_value_satoshi, \
                    output_spending_tx_hash \
                FROM " + self.outputsTable + " WHERE \
                    (output_time_spent >= %s AND output_time_spent < %s)), \
            sso AS (\
                SELECT \
                    sum(so.output_value_satoshi) AS value \
                FROM st JOIN so ON \
                    st.tx_hash=so.output_tx_hash), \
            ssi AS (\
                SELECT \
                    sum(si.output_value_satoshi) AS value \
                FROM st JOIN si ON \
                    st.tx_hash=si.output_spending_tx_hash) \
            SELECT \
                (coalesce(pow.value, 0) + coalesce(sso.value, 0) - coalesce(ssi.value, 0)) \
            FROM pow CROSS JOIN sso CROSS JOIN ssi", (minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate, minDate, maxDate))
        return result[0]
