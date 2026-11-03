#!/usr/bin/env bash
# apply.sh — пример применения SQLite-дельт клиентом (для справки).
#   ./apply.sh <category> <from_seq> <to_seq> <target.db>
#
# Патчи лежат в rolling-релизе как ehdb-<category>-run<seq>.delta.db.zstd.
# Обычный патч применяется к БД на seq = (seq патча - 1); нагоняющий патч
# может иметь from_seq старше (см. _ehdb_delta_meta) и применяется к этой базе.
set -euo pipefail
CAT="${1:?category}"; FROM="${2:?from_seq}"; TO="${3:?to_seq}"; DB="${4:?target db}"
REPO="${REPO:-fanout96/e-hentai-db}"
BASE_URL="${BASE_URL:-https://github.com/$REPO/releases/download/rolling}"
TOKEN="${GITHUB_TOKEN:-}"
CURL_AUTH=()
[ -n "$TOKEN" ] && CURL_AUTH=(-H "Authorization: Bearer $TOKEN")
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ehdb-apply.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

for ((seq=FROM+1; seq<=TO; seq++)); do
  echo "applying $CAT delta seq=$seq"
  DELTA_ZST="$TMP_DIR/ehdb-$CAT-run$seq.delta.db.zstd"
  DELTA_DB="$TMP_DIR/ehdb-$CAT-run$seq.delta.db"
  curl -fsSL "${CURL_AUTH[@]}" \
    "$BASE_URL/ehdb-$CAT-run$seq.delta.db.zstd" -o "$DELTA_ZST"
  zstd -t -q "$DELTA_ZST"
  zstd -dc "$DELTA_ZST" > "$DELTA_DB"
  python3 - "$DB" "$DELTA_DB" <<'PYEOF'
import json
import sqlite3
import sys

target_path, delta_path = sys.argv[1], sys.argv[2]
data_tables = ("gallery", "torrent", "gid_tid", "tag")

db = sqlite3.connect(target_path)
try:
    db.execute("ATTACH DATABASE ? AS delta", (delta_path,))

    meta = dict(db.execute("SELECT key, value FROM delta._ehdb_delta_meta"))
    if json.loads(meta.get("format", '""')) != "ehdb-sqlite-delta":
        raise SystemExit("not an ehdb sqlite delta")

    deletes = db.execute(
        "SELECT table_name, key_json FROM delta._ehdb_delta_deletes ORDER BY id"
    ).fetchall()
    for table_name, key_json in deletes:
        if table_name not in data_tables:
            raise SystemExit(f"unexpected table in delta: {table_name}")
        key = json.loads(key_json)
        if not key:
            continue
        where = " AND ".join(f'"{k}" = ?' for k in key)
        db.execute(f'DELETE FROM main."{table_name}" WHERE {where}', list(key.values()))

    for table_name in data_tables:
        cols = [r[1] for r in db.execute(f'PRAGMA main.table_info("{table_name}")')]
        if not cols:
            continue
        exists = db.execute(
            "SELECT 1 FROM delta.sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not exists:
            continue
        delta_cols = {r[1] for r in db.execute(f'PRAGMA delta.table_info("{table_name}")')}
        cols = [c for c in cols if c in delta_cols]
        if not cols:
            continue
        quoted = ",".join(f'"{c}"' for c in cols)
        db.execute(
            f'INSERT OR REPLACE INTO main."{table_name}" ({quoted}) '
            f'SELECT {quoted} FROM delta."{table_name}"'
        )
    db.commit()
finally:
    try:
        db.execute("DETACH DATABASE delta")
    except sqlite3.Error:
        pass
    db.close()
PYEOF
done
echo "done: $DB now at seq=$TO"
