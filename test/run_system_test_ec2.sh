#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv-test}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

HOST="${DSQL_HOST:-4btv7qq43k4ztw3tjubmbyf3su.dsql.us-east-2.on.aws}"
REGION="${AWS_REGION:-us-east-2}"
AUTH_TOKEN="${AUTH_TOKEN:-dev-shared-token}"

if [[ -x /usr/local/bin/aws ]]; then
  AWS_CLI=/usr/local/bin/aws
elif command -v aws >/dev/null 2>&1; then
  AWS_CLI="$(command -v aws)"
else
  echo "aws CLI not found; install aws cli and retry." >&2
  exit 1
fi

"$PYTHON_BIN" -m venv "$VENV_DIR"
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

pip install --upgrade pip
pip install \
  -r "$ROOT_DIR/test/requirements.txt" \
  -r "$ROOT_DIR/client/client-listener/requirements.txt" \
  -r "$ROOT_DIR/matchmaker/requirements.txt" \
  -r "$ROOT_DIR/executor/requirements.txt"

TOKEN="$("$AWS_CLI" dsql generate-db-connect-admin-auth-token \
  --hostname "$HOST" \
  --region "$REGION" \
  --output text)"

export TOKEN
ENC_TOKEN="$(python -c 'import os, urllib.parse; print(urllib.parse.quote(os.environ["TOKEN"], safe=""))')"
DB_DSN="postgresql://admin:${ENC_TOKEN}@${HOST}:5432/postgres?sslmode=require"

python "$ROOT_DIR/test/system_test.py" --db-dsn "$DB_DSN" --auth-token "$AUTH_TOKEN"
python "$ROOT_DIR/test/rpc_features_test.py" --db-dsn "$DB_DSN" --auth-token "$AUTH_TOKEN"
