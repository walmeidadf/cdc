#!/bin/bash
set -e
# lines to add at the end of pg_hba.conf files
# warning trust is enabled for all connections
# see https://www.postgresql.org/docs/12/auth-trust.html
echo "host all all all trust" >> /var/lib/postgresql/data/pg_hba.conf
echo "host replication postgres 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf