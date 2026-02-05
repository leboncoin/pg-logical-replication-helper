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


@dataclasses.dataclass
class DbInfos:
    def __init__(self, db_schemas, db_size, db_tables, schema_excluded_str):
        self.db_schemas = db_schemas
        self.db_size = db_size
        self.db_tables = db_tables
        self.schema_excluded_str = schema_excluded_str
