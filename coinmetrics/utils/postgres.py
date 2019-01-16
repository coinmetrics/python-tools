import psycopg2
import psycopg2.extras


class PostgresAccess(object):

    def __init__(self, dbHost, dbPort, dbName, dbUser, dbPassword):
        self.connection = psycopg2.connect("host=%s port=%s dbname=%s user=%s password=%s" % (dbHost, dbPort, dbName, dbUser, dbPassword))
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.close()

    def queryNoReturnCommit(self, text, params=None):
        self.cursor.execute(text, params)
        self.connection.commit()

    def queryNoReturnNoCommit(self, text, params=None):
        self.cursor.execute(text, params)

    def queryReturnOne(self, text, params=None):
        self.cursor.execute(text, params)
        return self.cursor.fetchone()

    def queryReturnAll(self, text, params=None):
        self.cursor.execute(text, params)
        return self.cursor.fetchall()

    def executeValues(self, sql, rows, batchSize):
        return psycopg2.extras.execute_values(self.cursor, sql, rows, page_size=batchSize)

    def commit(self):
        self.connection.commit()

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def getTableNames(self):
        return [row[0] for row in self.queryReturnAll("""
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_type = 'BASE TABLE' AND
                table_schema NOT IN ('pg_catalog', 'information_schema')""")]
