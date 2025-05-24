#!/usr/bin/env bash

GOPLS_PORT=${GOPLS_PORT:-4000}
GOPLS_PATH=$(goenv which gopls 2>/dev/null || command -v gopls 2>/dev/null || echo "gopls")
echo "gopls TCP wrapper: Attempting to use gopls at: $GOPLS_PATH" >&2
[[ $(which "${GOPLS_PATH}" 2>/dev/null) ]] || {
  echo "gopls TCP wrapper: gopls command not found into PATH." >&2
  exit 1
} && {
  echo "gopls TCP wrapper: Attempting to use gopls at: $(which gopls)"
  echo "Starting gopls in TCP listen mode on port ${GOPLS_PORT}..."
  gopls -listen=:${GOPLS_PORT} 2>&1 | tee /var/log/gopls.log &
  GOPLS_PID=$!
  echo "gopls started."
  wait $GOPLS_PID
  echo "gopls process (PID ${GOPLS_PID}) terminated."
}
