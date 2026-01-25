#!/bin/bash

echo "#### Dropping database..."
dropdb -U $POSTGRES_USER -w -f --if-exists metabasedb

echo "#### Restoring database..."
pg_restore -U $POSTGRES_USER -w -F c -C -d postgres /config/backup/metabasedb.dump