#!/bin/bash
set -e

USER="${HERITRIX_USER:-admin}"
PASS="${HERITRIX_PASSWORD:-admin123}"

echo "Starting Heritrix with user: ${USER}"
exec /opt/heritrix/bin/heritrix -a "${USER}:${PASS}" -b 0.0.0.0
