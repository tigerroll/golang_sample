#!/usr/bin/env bash

GOPLS_PATH=$(goenv which gopls 2>/dev/null || command -v gopls 2>/dev/null || echo "gopls")
echo "gopls TCP wrapper: Attempting to use gopls at: $GOPLS_PATH" >&2
[[ $(which "${GOPLS_PATH}" 2>/dev/null) ]] || {
  echo "gopls TCP wrapper: gopls command not found into PATH." >&2
  exit 1
}

exec socat TCP-LISTEN:4000,reuseaddr,fork EXEC:"$GOPLS_PATH"
