import pymssql


class SqlServer:

    def __init__(self, host, database, username, password) -> object:
        self.host = host
        self.database = database
        self.username = username
        self.password = password

    def __conn__(self):
        conn = pymssql.connect(server=self.host,
                               user=self.username,
                               password=self.password,
                               database=self.database)
        return conn

    def query_execute_count(self, sql_query):
        conn = self.__conn__()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        row = cursor.fetchone()
        return int(row[0])

    def query_execute_max_date(self, sql_query):
        conn = self.__conn__()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        row = cursor.fetchone()
        return row[0]

    def query_return_rows(self, sql_query, conn=None):
        if conn is None:
            conn = self.__conn__()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        list_query_result = list(cursor.fetchall())
        return list_query_result

    def execute_scalar_query(self, sql_query, conn):
        cursor = conn.cursor()
        cursor.execute(sql_query)
