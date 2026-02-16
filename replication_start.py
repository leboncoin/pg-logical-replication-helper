import os
import subprocess
import datetime
import sys

import psycopg
import re

from database import Database
from primary import Primary
from secondary import Secondary


def run_dump_restore_pre(conn_receiver_string, primary: Primary):
    dump_queries = primary.execute_dump()

    print(f"pg_restore pre begin")
    queries = dump_queries.replace("CREATE SCHEMA public;", "")
    # ignore "\restrict" and "\unrestrict" lines
    queries = re.sub("\\\\(un)?restrict.*\n", "", queries)
    
    # Connection to the database
    with psycopg.connect(conn_receiver_string) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(queries)
                conn.commit()

            except Exception as e:
                print(f"An error occurred: {e}")
                conn.rollback()

    print(f"run_dump_restore_pre end")


def run_dump_restore_post_onlypk(conn_sender_string, db_schemas, conn_receiver_string):
    command = [
        "pg_dump",
        "-d", conn_sender_string,
        "-Fp",
        "-T", "public.spatial_ref_sys",
        "--no-acl",
        f"--section=post-data"
    ]
    for schema in db_schemas:
        command.append("-n")
        command.append(schema)

    print(f" dump section post-data")
    print(" ".join(command))

    dump = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    dump_str = dump.stdout.read()
    
    print(f"pg_restore post début")
    # ignore "\restrict" and "\unrestrict" lines
    dump_str = re.sub("\\\\(un)?restrict.*\n", "", dump_str)
    splitlines = dump_str.splitlines()
    queries = ""
    for i in range(0, len(splitlines) - 1):
        if re.match(r'.*ADD CONSTRAINT.*PRIMARY KEY.*', splitlines[i]):
            queries = queries + \
                            splitlines[i - 1] + splitlines[i]

    # Connection to the database
    with psycopg.connect(conn_receiver_string) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(queries)
                conn.commit()

            except Exception as e:
                print(f"An error occurred: {e}")
                conn.rollback()

    print(f"run_dump_restore_post fin")


def run_dump_restore_post_without_pk(conn_sender_string, db_schemas, conn_receiver_string):
    command = [
        "pg_dump",
        "-d", conn_sender_string,
        "-Fp",
        "-T", "public.spatial_ref_sys",
        "--no-acl",
        f"--section=post-data"
    ]
    for schema in db_schemas:
        command.append("-n")
        command.append(schema)

    print(f" dump section post-data without primary keys")
    print(" ".join(command))

    dump = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    dump_str = dump.stdout.read()

    print(f"pg_restore post (without PK) début")
    # ignore "\restrict" and "\unrestrict" lines
    dump_str = re.sub("\\\\(un)?restrict.*\n", "", dump_str)
    splitlines = dump_str.splitlines()
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
    
    # Connection to the database
    with psycopg.connect(conn_receiver_string) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(queries)
                conn.commit()

            except Exception as e:
                print(f"An error occurred: {e}")
                conn.rollback()

        conn.rollback()
        
    print(f"run_dump_restore_post_without_pk fin")


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
        run_dump_restore_pre(conn_secondary, primary)

        # Section post-data
        run_dump_restore_post_onlypk(conn_primary, db_schemas, conn_secondary)

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
        run_dump_restore_post_without_pk(
            conn_primary, db_schemas, conn_secondary)

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
