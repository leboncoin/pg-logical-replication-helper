import datetime
import os
import sys

from database import Database
from primary import Primary
from replication import Replication
from secondary import Secondary


def main(name, conn_primary, db_primary, conn_secondary, db_secondary, list_schema_excluded):
    print(f"START SCRIPT")
    print(f"python {name} {conn_primary} {db_primary} {conn_secondary} {db_secondary}")

    # Logs settings
    today = datetime.datetime.now().strftime("%Y%m%d-%H-%M-%S")
    print(f"\n\nStarting script : {name} at {today}\n")

    primary = Primary(Database(conn_primary, db_primary), list_schema_excluded)
    secondary = Secondary(Database(conn_secondary, db_secondary))
    replication = Replication(primary, secondary)
    replication.run(name, today)


if __name__ == '__main__':
    # Initialisation of replication
    script_name = os.path.basename(__file__)
    connection_primary = sys.argv[1]
    db_name_primary = sys.argv[2]
    connection_secondary = sys.argv[3]
    db_name_secondary = sys.argv[4] if len(sys.argv) > 4 else db_name_primary
    schema_excluded_list = sys.argv[5] if len(sys.argv) > 5 else None
    schema_excluded = schema_excluded_list.split(',')

    main(script_name, connection_primary, db_name_primary, connection_secondary, db_name_secondary, schema_excluded)
