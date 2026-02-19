import os
import time

from database import Database

WAITING_PROGRESS_IN_SECONDS = 10


class Secondary:
    def __init__(self, db: Database):
        self.db = db

    def get_subscription_name(self, db_primary: str) -> str | None:
        query = f"select subname from pg_subscription where subname like 'subscription_{db_primary}_%'"
        print(f"psql \"{self.db.conn_string}\" --no-align -tc \"{query}\"")
        results = self.db.execute_query(query)
        if results is None:
            return None
        if not results:
            return ""

        return results[0][0]

    def create_subscription(self, unique_name: str):
        # Create subscription on secondary
        subscription_name = f"subscription_{unique_name}"
        print(
            f"Create subscription on secondary {self.db.conn_string} database {self.db.db_name}")
        # Get the primary db connexion string fron environment
        connection_primary_full = os.environ.get('CONN_DB_PRIMARY_FULL')
        self.db.execute_query(
            f"CREATE SUBSCRIPTION {subscription_name} CONNECTION '{connection_primary_full}' PUBLICATION publication_{unique_name} with (copy_data=true, create_slot=true, enabled=true, slot_name='{subscription_name}');",
            fetch=False)

    def wait_first_step_of_replication(self):
        print(
            "The first step of logical replication is not finished - retrying later")
        while True:
            try:
                results = self.db.execute_query("select a.* from pg_subscription_rel a inner join pg_class on srrelid=pg_class.oid where relname <> 'spatial_ref_sys' and srsubstate <> 'r';")
                if not results:
                    break

                    # Log progress
                progress_query = """
                                 with ready as (select count(a.*) as ready
                                                from pg_subscription_rel a
                                                         inner join pg_class on srrelid = pg_class.oid
                                                where relname <> 'spatial_ref_sys'
                                                  and srsubstate = 'r'),
                                      total as (select count(a.*) as total
                                                from pg_subscription_rel a
                                                         inner join pg_class on srrelid = pg_class.oid
                                                where relname <> 'spatial_ref_sys')
                                 select *
                                 from ready,
                                      total; \
                                 """
                results = self.db.execute_query(progress_query)
                print(
                    f"Replication progress : {results[0][0]}/{results[0][1]}")

                time.sleep(WAITING_PROGRESS_IN_SECONDS)
            except:
                # If the query fails, it means there are no more tables in non-ready state
                break

    def disable_subscription(self, subscription_name):
        print(f"Disable subscription on {self.db.conn_string}")
        self.db.execute_query(f"ALTER SUBSCRIPTION {subscription_name} DISABLE;", fetch=False)

    def enable_subscription(self, subscription_name):
        print(f"Enable subscription on {self.db.conn_string}")
        self.db.execute_query(f"ALTER SUBSCRIPTION {subscription_name} ENABLE;", fetch=False)
