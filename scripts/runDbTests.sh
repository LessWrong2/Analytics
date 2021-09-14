#! /bin/bash
set -eox pipefail

source .env

PGPASSWORD=$PGPASSWORD bash -c "pg_prove -d $DATABASE_NAME -h $DATABASE_HOST -U $DATABASE_USER --verbose $1"
