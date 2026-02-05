import sys

import psycopg
from psycopg import Error


class Database:
    def __init__(self, conn_string, db_name):
        self.conn_string = conn_string
        self.db_name = db_name

    def get_db_connection(self):
        try:
            conn = psycopg.connect(self.conn_string, autocommit=True)
            return conn
        except psycopg.Error as e:
            print(
                f"Error {e} on connection string {self.conn_string}", file=sys.stderr)
            sys.exit(1)

    def execute_query(self, query, fetch=True):
        conn = self.get_db_connection()
        if conn is None:
            return None

        try:
            with conn.cursor() as cur:
                cur.execute(query)
                if fetch:
                    results = cur.fetchall()
                    return results
        except Error as e:
            print(
                f"Error {e} with query '{query}' on host '{self.conn_string}'", file=sys.stderr)
            return None
        finally:
            conn.close()


