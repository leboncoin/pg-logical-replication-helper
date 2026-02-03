import dataclasses
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

    def retrieve_db_infos(self, list_schema_excluded) -> DbInfos:
        schema_excluded_str = ""
        if list_schema_excluded is None:
            schema_query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%'"
        else:
            schema_excluded_str = ",".join(
                [f"'{schema}'" for schema in list_schema_excluded])
            schema_query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%' AND schema_name NOT IN ({schema_excluded_str});"
        results = self.execute_query(schema_query)
        db_schemas = None
        if results and results[0]:
            db_schemas = [schema[0] for schema in results]

        results = self.execute_query(
            f"SELECT pg_size_pretty(pg_database_size('{self.db_name}'))")
        db_size = None
        if results and results[0]:
            db_size = results[0][0]

        results = self.execute_query(
            "SELECT count(*) from pg_stat_user_tables")
        db_tables = None
        if results and results[0]:
            db_tables = results[0][0]
        return DbInfos(db_schemas, db_size, db_tables, schema_excluded_str)


@dataclasses.dataclass
class DbInfos:
    def __init__(self, db_schemas, db_size, db_tables, schema_excluded_str):
        self.db_schemas = db_schemas
        self.db_size = db_size
        self.db_tables = db_tables
        self.schema_excluded_str = schema_excluded_str
