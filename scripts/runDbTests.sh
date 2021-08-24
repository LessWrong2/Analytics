#! /bin/bash
set -eox pipefail

source .env

PGPASSWORD=$PGPASSWORD bash -c "pg_prove -d forumanalytics -h $DATABASE_HOST -U postgres --verbose $1"
