#!/usr/bin/env bash
set -euo pipefail

HOST="${DSQL_HOST:-4btv7qq43k4ztw3tjubmbyf3su.dsql.us-east-2.on.aws}"
REGION="${AWS_REGION:-us-east-2}"
AUTH_TOKEN="${AUTH_TOKEN:-dev-shared-token}"

TOKEN="$(/usr/local/bin/aws dsql generate-db-connect-admin-auth-token \
  --hostname "$HOST" \
  --region "$REGION" \
  --output text)"

export TOKEN
ENC_TOKEN="$(python3 -c 'import os, urllib.parse; print(urllib.parse.quote(os.environ["TOKEN"], safe=""))')"
DB_DSN="postgresql://admin:${ENC_TOKEN}@${HOST}:5432/postgres?sslmode=require"

python3 test/system_test.py --db-dsn "$DB_DSN" --auth-token "$AUTH_TOKEN"
