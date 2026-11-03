#!/usr/bin/env bash
# Внешний триггер fork-delta-sync. Запускать из cron/systemd timer:
#   GH_TOKEN=... /path/to/trigger.sh
# Нужен токен с правом contents:write (classic PAT: repo).
set -euo pipefail

REPO="${REPO:-fanout96/e-hentai-db}"
TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
[ -n "$TOKEN" ] || { echo "GH_TOKEN is required" >&2; exit 1; }

curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/$REPO/dispatches" \
  -d '{"event_type":"fork-delta-sync"}'
echo "fork-delta-sync dispatched to $REPO"
