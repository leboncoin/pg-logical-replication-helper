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
