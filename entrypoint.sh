#!/bin/bash
set -euo pipefail

audit_path=/var/log/webshell
mkdir -p "$audit_path"
export AUDIT_LOG_PATH="${audit_path}/sqs.audit"

_term() {
  echo "caught shutdown signal, stopping aws tool"
  kill -TERM "${child:-}" 2>/dev/null || true
}

trap _term SIGTERM
trap _term SIGINT

PORT="${PORT:-8085}"
TOKEN="${TOKEN:-aws-tools}"

echo "starting cdp-aws-tools PORT=${PORT} TOKEN=${TOKEN}"
uvicorn app.main:app --host 0.0.0.0 --port "$PORT" &
child=$!

child_exit=0
wait "$child" || child_exit=$?
wait "$child" 2>/dev/null || true

if [ -n "${AUDIT_UPLOAD_URL:-}" ]; then
  url="$(printf '%s' "$AUDIT_UPLOAD_URL" | base64 -d)"
  file_to_upload="${audit_path}/audit.tgz"
  find "$audit_path" -type f -name "*.audit" -exec tar --no-recursion --transform 's|^.*/||' -czf "$file_to_upload" {} +

  if [ -f "$file_to_upload" ]; then
    echo "uploading audit file [${file_to_upload}] to s3"
    curl --fail --silent --show-error --noproxy '*' --request PUT --upload-file "${file_to_upload}" "$url"
  else
    echo "no audit file found: [${file_to_upload}]"
  fi
else
  echo "no AUDIT_UPLOAD_URL set, skipping audit upload"
fi

exit "$child_exit"
