#!/bin/sh
# Container entrypoint for `dash` (§14.1 optional Docker image).
# On first start (no SQLite database yet) the Installation is initialized. With no
# DASH_ADMIN_PASSWORD a random Administrator password is generated and printed ONCE to the
# container log; there is no default password. Set DASH_INIT_FROM_FIXTURE to seed the Workspace
# (e.g. /opt/dash/fixtures/engineering-command-center).
set -eu
DATA="${DASH_DATA_DIR:-/data}"
if [ "${1:-serve}" = "serve" ] && [ ! -f "$DATA/dash.db" ]; then
  if [ -n "${DASH_INIT_FROM_FIXTURE:-}" ]; then
    dash init --from-fixture "$DASH_INIT_FROM_FIXTURE"
  else
    dash init
  fi
fi
exec dash "$@"
