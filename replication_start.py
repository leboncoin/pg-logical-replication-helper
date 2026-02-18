import datetime
import os
import re
import sys

from database import Database
from primary import Primary
from secondary import Secondary


def run_dump_restore_pre(primary: Primary, secondary: Secondary):
    print(f"pg_restore pre begin")
    
    with primary.execute_dump("pre-data") as dump:
        dump_queries = dump.stdout.read()
        
        queries = dump_queries.replace("CREATE SCHEMA public;", "")
        # ignore "\restrict" and "\unrestrict" lines
        queries = re.sub("\\\\(un)?restrict.*\n", "", queries)
    
        secondary.db.execute_query_rollback_on_error(queries)

    print(f"run_dump_restore_pre end")


def run_dump_restore_post_only_pk(primary: Primary, secondary: Secondary):
    print(f"pg_restore post begin")
    
    with primary.execute_dump("post-data") as dump:
        dump_queries = dump.stdout.read()
        
        # ignore "\restrict" and "\unrestrict" lines
        dump_queries = re.sub("\\\\(un)?restrict.*\n", "", dump_queries)
        splitlines = dump_queries.splitlines()
        queries = ""
        for i in range(0, len(splitlines) - 1):
            if re.match(r'.*ADD CONSTRAINT.*PRIMARY KEY.*', splitlines[i]):
                queries = queries + \
                          splitlines[i - 1] + splitlines[i]
    
        secondary.db.execute_query_rollback_on_error(queries)

    print(f"run_dump_restore_post end")


def run_dump_restore_post_without_pk(primary: Primary, secondary: Secondary):
    print(f"pg_restore post (without PK) begin")
    
    with primary.execute_dump("post-data") as dump:
        dump_queries = dump.stdout.read()
        
        # ignore "\restrict" and "\unrestrict" lines
        dump_queries = re.sub("\\\\(un)?restrict.*\n", "", dump_queries)
        splitlines = dump_queries.splitlines()
        queries = ""
        line_before = ""
        for i in range(0, len(splitlines) - 1):
            # Skip primary key constraints
            if re.match(r'.*ADD CONSTRAINT.*PRIMARY KEY.*', splitlines[i]):
                line_before = ""
                continue
            else:
                queries += line_before
                line_before = splitlines[i] + "\n"

        queries += line_before
    
        secondary.db.execute_query_rollback_on_error(queries)

    print(f"run_dump_restore_post_without_pk end")


def main(name, conn_primary, db_primary, conn_secondary, db_secondary, list_schema_excluded):
    print(f"START SCRIPT")
    print(f"python {name} {conn_primary} {db_primary} {conn_secondary} {db_secondary}")

    # Logs settings
    today = datetime.datetime.now().strftime("%Y%m%d-%H-%M-%S")
    print(f"\n\nStarting script : {name} at {today}\n")

    # Retrieve DB Infos
    primary = Primary(Database(conn_primary, db_primary), list_schema_excluded)
    db_schemas = primary.db_infos.db_schemas

    secondary = Secondary(Database(conn_secondary, db_secondary))

    # Check if replication is already started
    results = secondary.get_subscription_name(db_primary)
    if results is None:
        print("end")
        return

    # Check if replication is already started
    if not results:
        print("Replication not in progress")
        print(f"{today} - Starting process : {name} {conn_primary} {db_primary} - {conn_secondary} database {db_secondary}")

        primary.create_replication_user()

        # Section pre-data
        run_dump_restore_pre(primary, secondary)

        # Section post-data
        run_dump_restore_post_only_pk(primary, secondary)

        date_start = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_name = f"{db_primary}_{date_start}"

        primary.create_publication(unique_name)

        secondary.create_subscription(unique_name)

    # Check if replication is still running
    results = secondary.get_subscription_name(db_primary)
    if results:
        subscription_name = results[0][0]
        print(
            f"Check if first step of replication is done - db {db_secondary} on host {conn_secondary} from {conn_primary} database {db_primary}")
        secondary.wait_first_step_of_replication()

        # Disable subscription
        secondary.disable_subscription(subscription_name)

        # Restore post section without primary keys
        print("Restore post section - without primary key")
        run_dump_restore_post_without_pk(primary, secondary)

        # Enable subscription
        secondary.enable_subscription(subscription_name)

        end_time = datetime.datetime.now().strftime("%Y%m%d-%H-%M-%S")
        print(f"end={end_time}")
    else:
        print("No replication running, exiting")

    print("end")


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
