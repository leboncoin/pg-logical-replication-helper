import dataclasses

from database import Database


class Primary:
    def __init__(self, db: Database):
        self.db = db

    def retrieve_db_infos(self, list_schema_excluded) -> DbInfos:
        schema_excluded_str = ""
        if list_schema_excluded is None:
            schema_query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%'"
        else:
            schema_excluded_str = ",".join(
                [f"'{schema}'" for schema in list_schema_excluded])
            schema_query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%' AND schema_name NOT IN ({schema_excluded_str});"
        results = self.db.execute_query(schema_query)
        db_schemas = None
        if results and results[0]:
            db_schemas = [schema[0] for schema in results]

        results = self.db.execute_query(
            f"SELECT pg_size_pretty(pg_database_size('{self.db.db_name}'))")
        db_size = None
        if results and results[0]:
            db_size = results[0][0]

        results = self.db.execute_query(
            "SELECT count(*) from pg_stat_user_tables")
        db_tables = None
        if results and results[0]:
            db_tables = results[0][0]
        return DbInfos(db_schemas, db_size, db_tables, schema_excluded_str)

    def create_publication(self, db_infos: DbInfos, unique_name: str):
        print(
            f"Create publication on primary {self.db.conn_string} database {self.db.db_name}")
    
        self.db.execute_query(f"CREATE PUBLICATION publication_{unique_name};", 
                              fetch=False)
        # Add tables to publication
        query_publication = f"select schemaname, relname from pg_stat_user_tables where relname <> 'spatial_ref_sys'"
        if db_infos.schema_excluded_str != "":
            query_publication = query_publication + f" AND schemaname NOT IN ({db_infos.schema_excluded_str})"
        results = self.db.execute_query(query_publication)
        if results:
            for schema, table in results:
                print(
                    f"Add table {schema}.{table} to publication {unique_name}")
                self.db.execute_query(f"ALTER PUBLICATION publication_{unique_name} ADD TABLE {schema}.{table};", 
                                      fetch=False)
    

@dataclasses.dataclass
class DbInfos:
    def __init__(self, db_schemas, db_size, db_tables, schema_excluded_str):
        self.db_schemas = db_schemas
        self.db_size = db_size
        self.db_tables = db_tables
        self.schema_excluded_str = schema_excluded_str
