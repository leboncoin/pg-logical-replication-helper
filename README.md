[![Tests Status](https://raw.githubusercontent.com/laurentdutheil/pg-logical-replication-helper/badges/junit-badge.svg?raw=true)]()
[![Coverage Status](https://raw.githubusercontent.com/laurentdutheil/pg-logical-replication-helper/badges/coverage-badge.svg?raw=true)]()

# PostgreSQL Logical Replication Helper

## Overview

This script automates the setup of PostgreSQL logical replication between two database instances. It handles the entire process from creating necessary user and permissions to configuring publications and subscriptions.

## Features

- Creates replication users with appropriate permissions
- Copying structures and schema from source to target database
- Configures publications on the source database
- Establishes subscriptions on the target database
- Monitors replication progress
- Supports schema exclusion for selective replication
- Create index after full replication of data

## Prerequisites

- PostgreSQL instances with logical replication enabled
    wal_level=logical or rds.logical_replication=true (for AWS)
- Database source and target already created
- `pg_dump` and `psycopg` Python package installed
- Appropriate connection credentials for both source and target databases
    the users could select the entire of the source database and need the right to create on objets on the target database

## Usage

```bash
python replication_start.py <connection_primary> <db_name_primary> <connection_secondary> <db_name_secondary> <create_db> <schema_excluded_list>
```
### Parameters

- `connection_primary`: Connection string for the source database
- `db_name_primary`: Name of the source database
- `connection_secondary`: Connection string for the target database
- `db_name_secondary`: (Optional) Name of the target database (defaults to source database name)
- `create_db`: (Optional) Set to "true" if the target database needs to be created
- `schema_excluded_list`: (Optional) Comma-separated list of schemas to exclude from replication

### Environment Variables

- `CONN_DB_PRIMARY_FULL`: Full connection string for the primary database including credentials
