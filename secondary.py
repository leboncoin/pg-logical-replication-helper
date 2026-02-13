import os
import sys
from typing import Any

from database import Database


class Secondary:
    def __init__(self, db: Database):
        self.db = db

    def get_subscription_name(self, db_primary) -> Any | None:
        query =f"select subname from pg_subscription where subname like 'subscription_{db_primary}_%'"
        print(f"psql \"{self.db.conn_string}\" --no-align -tc \"{query}\"")
        results = self.db.execute_query(query)
        if results is None:
            print(
                f"Error on query {query} on host {self.db.conn_string}", file=sys.stderr)
            return None
        
        return results

    def create_subscription(self, unique_name: str):
        # Create subscription on secondary
        subscription_name = f"subscription_{unique_name}"
        print(
            f"Create subscription on secondary {self.db.conn_string} database {self.db.db_name}")
        # Get the primary db connexion string fron environment
        connection_primary_full = os.environ.get('CONN_DB_PRIMARY_FULL')
        self.db.execute_query(f"CREATE SUBSCRIPTION {subscription_name} CONNECTION '{connection_primary_full}' PUBLICATION publication_{unique_name} with (copy_data=true, create_slot=true, enabled=true, slot_name='{subscription_name}');",
                      fetch=False)
