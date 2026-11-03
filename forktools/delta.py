#!/usr/bin/env python3
"""
delta.py — генератор инкрементальных патчей для форка URenko/e-hentai-db.

Сравнивает два дампа SQLite (previous и current), скачанные из релиза `nightly`
upstream-репозитория, и формирует:
  * полные снапшоты по категориям (ehdb-<category>-run<seq>.db.zstd)
  * SQLite-дельты по категориям (ehdb-<category>-run<seq>.delta.db.zstd)
  * manifest.json (публикуется последним — точка атомарного коммита)

Дельта считается как diff между дампами (источник истины). Логи upstream
используются как дополнительный changelog
(got X new galleries of Y to Z / root gid of [new] is [old]).

SQLite-дельта — это обычная SQLite БД с теми же таблицами (gallery, torrent,
gid_tid, tag), где лежат только изменённые/новые строки, плюс служебные таблицы:
  _ehdb_delta_meta    (key, value)          — seq/from_seq/category/counts/...
  _ehdb_delta_deletes (table_name, key_json) — удалённые строки (ключ в JSON)

Применение: DELETE по _ehdb_delta_deletes, затем INSERT OR REPLACE строк из
таблиц дельты в целевую БД (см. apply.sh).

Использование:
  python3 delta.py --old prev.db --new cur.db --out dist \
      --config config/categories.json --prev-manifest manifest.json --seq 42 \
      [--changelog changelog.txt]
"""

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path


def log(msg):
    print(f"[delta] {msg}", flush=True)


def zstd_compress(src: Path, dst: Path, level: int):
    subprocess.run(["zstd", f"-{level}", "-T0", "-q", "--force", str(src), "-o", str(dst)],
                   check=True)


def table_columns(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def pk_columns(conn, table):
    cols = table_columns(conn, table)
    pk = [r[1] for r in conn.execute(f"PRAGMA table_info({table})") if r[5] > 0]
    return pk or cols[:1]


def fetch_map(conn, table, cols, keycols):
    """map: tuple(key values) -> full row tuple."""
    idx = [cols.index(k) for k in keycols]
    m = {}
    for row in conn.execute(f"SELECT {','.join(cols)} FROM {table}"):
        m[tuple(row[i] for i in idx)] = row
    return m


# gid -> category: стрим-словари (~2.5М записей, ~150MB RAM) вместо
# точечных SELECT'ов по 1.5М+ раз (на 1ГБ дампе это был часовой I/O — run #7
# упёрся в лимит 6ч). Заполняется один раз при первом обращении.
_cat_maps = {}


def _build_cat_map(conn):
    m = {}
    for gid, cat in conn.execute("SELECT gid, category FROM gallery"):
        m[gid] = cat
    return m


def get_cat_map(conn):
    key = id(conn)
    m = _cat_maps.get(key)
    if m is None:
        m = _build_cat_map(conn)
        _cat_maps[key] = m
    return m


def lookup_category(new_conn, old_conn, gid):
    """gid -> category из in-memory карты (для удалённых gid ищем в old)."""
    cat = get_cat_map(new_conn).get(gid)
    if cat is None and old_conn is not None:
        cat = get_cat_map(old_conn).get(gid)
    return cat if cat is not None else "unknown"


def make_category_db(src_conn, dst_path, cat, src_file):
    """Собирает standalone БД одной категории (gallery + связанные torrent/gid_tid/tag).

    src_file — путь к файлу исходной БД; ATTACH используется, чтобы JOIN по
    временной таблице gid шёл в conn, а не materialize всю таблицу на каждую
    категорию (O(N×cats) сканов убивает лимит 6ч на 1ГБ дампе).
    """
    if os.path.exists(dst_path):
        os.remove(dst_path)
    conn = sqlite3.connect(dst_path)
    # схема — из исходной БД (сохраняем индексы/триггеры тоже)
    for name, sql in src_conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL "
            "AND name NOT LIKE 'sqlite_%'"):
        conn.execute(sql)
    for name, sql in src_conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type IN ('index','trigger') AND sql IS NOT NULL"):
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()

    cols_g = table_columns(src_conn, "gallery")
    gi_sel = ",".join(cols_g)
    ins_g = f"INSERT OR REPLACE INTO gallery VALUES ({','.join('?' * len(cols_g))})"
    batch = []
    gcount = 0
    # стрим по упорядоченному индексу PK gid — O(limit) RAM
    for row in src_conn.execute(
            f"SELECT {gi_sel} FROM gallery WHERE lower(trim(category))=? ORDER BY gid", (cat,)):
        batch.append(row)
        gcount += 1
        if len(batch) >= 5000:
            conn.executemany(ins_g, batch)
            batch = []
    if batch:
        conn.executemany(ins_g, batch)
    conn.commit()

    try:
        conn.execute("ATTACH DATABASE ? AS srcdb", (f"file:{src_file}?mode=ro",))
        # временная таблица gid-ов категории + JOIN — покрывает случай, когда
        # индекс по category отсутствует (полный скан gallery один раз, cheap)
        conn.execute("CREATE TEMP TABLE _gids(gid INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO _gids SELECT gid FROM main.gallery")
        conn.commit()
        for t in ("torrent", "gid_tid"):
            cols = table_columns(src_conn, t)
            sel = ",".join(f"srcdb.{t}.{c}" for c in cols)
            ins = f"INSERT OR REPLACE INTO {t} VALUES ({','.join('?' * len(cols))})"
            batch = []
            for row in conn.execute(
                    f"SELECT {sel} FROM srcdb.{t} JOIN _gids ON srcdb.{t}.gid = _gids.gid"):
                batch.append(row)
                if len(batch) >= 5000:
                    conn.executemany(ins, batch)
                    batch = []
            if batch:
                conn.executemany(ins, batch)
            conn.commit()
        # tag — маленькая таблица, копируем целиком
        cols_t = table_columns(src_conn, "tag")
        conn.executemany(
            f"INSERT OR IGNORE INTO tag VALUES ({','.join('?' * len(cols_t))})",
            src_conn.execute(f"SELECT {','.join(cols_t)} FROM tag"))
        conn.commit()
    finally:
        try:
            conn.execute("DETACH DATABASE srcdb")
        except Exception:
            pass
    conn.close()
    return gcount


DATA_TABLES = ("gallery", "torrent", "gid_tid", "tag")
DELTA_META_TABLE = "_ehdb_delta_meta"
DELTA_DELETES_TABLE = "_ehdb_delta_deletes"
DELTA_FORMAT = "ehdb-sqlite-delta"
DELTA_FORMAT_VERSION = 1


def quote_ident(name):
    return '"' + str(name).replace('"', '""') + '"'


def create_delta_db(dst_path, schema_conn, recs, meta):
    """Собирает SQLite-дельту: только изменённые строки + список удалений."""
    if os.path.exists(dst_path):
        os.remove(dst_path)
    conn = sqlite3.connect(dst_path)
    try:
        for table in DATA_TABLES:
            row = schema_conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not row or not row[0]:
                raise RuntimeError(f"table {table!r} is missing in --new dump")
            # Схема таблицы без индексов/триггеров: дельта — переносимый контейнер.
            conn.execute(row[0])

        conn.execute(
            f"CREATE TABLE {DELTA_META_TABLE} "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            f"CREATE TABLE {DELTA_DELETES_TABLE} "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "table_name TEXT NOT NULL, key_json TEXT NOT NULL)"
        )

        for rec in recs:
            table = rec.get("table")
            if table not in DATA_TABLES:
                raise RuntimeError(f"unexpected delta table: {table!r}")
            if rec["op"] == "upsert":
                row = rec["row"]
                cols = list(row.keys())
                conn.execute(
                    f"INSERT OR REPLACE INTO {quote_ident(table)} "
                    f"({','.join(quote_ident(c) for c in cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})",
                    [row[c] for c in cols],
                )
            elif rec["op"] == "delete":
                conn.execute(
                    f"INSERT INTO {DELTA_DELETES_TABLE} (table_name, key_json) VALUES (?, ?)",
                    (table, json.dumps(rec["key"], ensure_ascii=False, separators=(",", ":"))),
                )
            else:
                raise RuntimeError(f"unexpected delta op: {rec['op']!r}")

        conn.executemany(
            f"INSERT INTO {DELTA_META_TABLE} (key, value) VALUES (?, ?)",
            [(k, json.dumps(v, ensure_ascii=False)) for k, v in meta.items()],
        )
        conn.commit()
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default=None, help="previous full dump (sqlite); omit for first run")
    ap.add_argument("--new", required=True, help="current full dump (sqlite)")
    ap.add_argument("--out", default="dist")
    ap.add_argument("--config", default="config/categories.json")
    ap.add_argument("--prev-manifest", default=None)
    ap.add_argument("--seq", type=int, required=True)
    ap.add_argument("--changelog", default=None)
    ap.add_argument("--asset-id", default=None,
                    help="upstream nightly asset id (published state marker)")
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    enabled = {c.lower() for c in cfg.get("enabled_categories", [])}
    all_cats = cfg.get("all_categories", True) or "*" in enabled
    include_private = cfg.get("include_private_categories", False)
    level = cfg.get("compression_level", 12)
    retention = cfg.get("retention", {"patch_generations": 30, "full_snapshots": 7})

    new_conn = sqlite3.connect(f"file:{args.new}?mode=ro", uri=True)
    # sanity check: пустой/битый дамп (например, недокачанный prev из кэша) —
    # в diff-режим использовать нельзя, переходим в first-run (полные снапшоты)
    try:
        _n_gal = new_conn.execute("SELECT COUNT(*) FROM gallery").fetchone()[0]
    except sqlite3.DatabaseError as e:
        raise SystemExit(f"[delta] FATAL: --new dump is corrupt or empty: {e}")
    if _n_gal == 0:
        log("WARNING: new dump has 0 galleries?!")
    old_path = Path(args.old) if args.old else None
    # first-run = нет prev-файла ИЛИ он не читается как sqlite с таблицей gallery.
    # По размеру НЕ судим: на синтетических/маленьких дампах проверка >1MB
    # ошибочно включала first-run и отключала диффы.
    first_run = not (old_path and old_path.exists())
    if old_path and old_path.exists() and not first_run:
        try:
            _c = sqlite3.connect(f"file:{old_path}?mode=ro", uri=True)
            _c.execute("SELECT COUNT(*) FROM gallery").fetchone()
            _c.close()
        except sqlite3.DatabaseError as e:
            log(f"WARNING: previous dump unusable ({e}) — falling back to first-run mode")
            first_run = True
    if first_run:
        log("first run / no usable previous dump — full snapshots only, no patches")
    old_conn = None if first_run else sqlite3.connect(f"file:{args.old}?mode=ro", uri=True)
    def rows(conn, sql):
        """стрим строк таблицы; для first_run (old=None) — пустой итератор."""
        if conn is None:
            return iter(())
        return conn.execute(sql)

    def cat_allowed(cat):
        cat_n = (cat or "").strip().lower()
        if not include_private and cat_n == "private":
            return False
        if all_cats:
            return True
        return cat_n in enabled

    seq = args.seq
    prev_manifest = None
    if args.prev_manifest and Path(args.prev_manifest).exists():
        prev_manifest = json.loads(Path(args.prev_manifest).read_text())

    max_gid = new_conn.execute("SELECT MAX(gid) FROM gallery").fetchone()[0]
    min_gid_new = None

    # whitelist явных категорий для снапшотов (без '*'); 'misc' всегда включается.
    # Патчи для категорий вне whitelist адресуются в 'misc' (cat_bucket ниже),
    # т.к. полный перебор всех ~25 категорий давал O(N×cats) сканов на 1ГБ дампе.
    explicit = {c for c in enabled if c not in ("*", "all")}

    changes_by_cat = {}
    stats = {"upserts": 0, "deletes": 0}

    def emit(cat, rec):
        cat_n = (cat or "unknown").strip().lower()
        changes_by_cat.setdefault(cat_n, []).append(rec)

    # ---------------- gallery ----------------
    cols_g = table_columns(new_conn, "gallery")
    sel_g = f"SELECT {','.join(cols_g)} FROM gallery ORDER BY gid"
    ci_cat = cols_g.index("category")

    def cat_bucket(cat):
        """категория строки для адресации патча: 'misc' — ловушка всего, что не
        попало в whitelist снапшотов (включая NULL и переименованные категории).
        Иначе патч 'переехавшей' галереи терялся (gid оставался в prev-снапшоте
        misc) — см. smoke-test."""
        c = (cat or "").strip().lower()
        if all_cats or c in explicit or c == "misc":
            return c
        return "misc"

    o_cur = rows(old_conn, sel_g)
    n_cur = iter(new_conn.execute(sel_g))
    try:
        o = next(o_cur)
    except StopIteration:
        o = None
    try:
        n = next(n_cur)
    except StopIteration:
        n = None

    while o is not None or n is not None:
        if n is None or (o is not None and o[0] < n[0]):
            # удалённая строка gallery
            if cat_allowed(o[ci_cat]):
                emit(cat_bucket(o[ci_cat]), {"op": "delete", "table": "gallery", "key": {"gid": o[0]}})
                stats["deletes"] += 1
            o = next(o_cur, None)
        elif o is None or n[0] > o[0]:
            # новая строка
            if cat_allowed(n[ci_cat]):
                min_gid_new = n[0] if min_gid_new is None else min(min_gid_new, n[0])
                emit(cat_bucket(n[ci_cat]), {"op": "upsert", "table": "gallery",
                                             "row": dict(zip(cols_g, n))})
                stats["upserts"] += 1
            n = next(n_cur, None)
        else:
            # одинаковый gid — сравниваем целиком
            if o != n and cat_allowed(n[ci_cat]):
                # delete по старому bucket + upsert по новому — корректно
                # обрабатывает смену категории галереи
                if cat_allowed(o[ci_cat]):
                    emit(cat_bucket(o[ci_cat]), {"op": "delete", "table": "gallery", "key": {"gid": o[0]}})
                    stats["deletes"] += 1
                emit(cat_bucket(n[ci_cat]), {"op": "upsert", "table": "gallery",
                                             "row": dict(zip(cols_g, n))})
                stats["upserts"] += 1
            o = next(o_cur, None)
            n = next(n_cur, None)

    # ---------------- torrent ---------------- (PK id, стрим по id; cat из gid->category на лету)
    cols_t = table_columns(new_conn, "torrent")
    gi_t = cols_t.index("gid")
    sel_t = f"SELECT {','.join(cols_t)} FROM torrent ORDER BY id"
    o_cur = rows(old_conn, sel_t)
    n_cur = iter(new_conn.execute(sel_t))
    o = next(o_cur, None)
    n = next(n_cur, None)
    while o is not None or n is not None:
        if n is None or (o is not None and o[0] < n[0]):
            cat = lookup_category(new_conn, old_conn, o[gi_t])
            if cat_allowed(cat):
                emit(cat_bucket(cat), {"op": "delete", "table": "torrent", "key": {"id": o[0]}})
                stats["deletes"] += 1
            o = next(o_cur, None)
        elif o is None or n[0] > o[0]:
            cat = lookup_category(new_conn, old_conn, n[gi_t])
            if cat_allowed(cat):
                emit(cat_bucket(cat), {"op": "upsert", "table": "torrent",
                           "row": dict(zip(cols_t, n))})
                stats["upserts"] += 1
            n = next(n_cur, None)
        else:
            if o != n:
                cat = lookup_category(new_conn, old_conn, n[gi_t])
                if cat_allowed(cat):
                    emit(cat_bucket(cat), {"op": "upsert", "table": "torrent",
                               "row": dict(zip(cols_t, n))})
                    stats["upserts"] += 1
            o = next(o_cur, None)
            n = next(n_cur, None)

    # ---------------- gid_tid ---------------- (PK (gid,tid), стрим по gid,tid)
    cols_gt = table_columns(new_conn, "gid_tid")
    sel_gt = f"SELECT {','.join(cols_gt)} FROM gid_tid ORDER BY gid, tid"

    def gt_key(r):
        return (r[cols_gt.index("gid")], r[cols_gt.index("tid")])

    o_cur = rows(old_conn, sel_gt)
    n_cur = iter(new_conn.execute(sel_gt))
    o = next(o_cur, None)
    n = next(n_cur, None)
    while o is not None or n is not None:
        if n is None or (o is not None and gt_key(o) < gt_key(n)):
            cat = lookup_category(new_conn, old_conn, gt_key(o)[0])
            if cat_allowed(cat):
                emit(cat_bucket(cat), {"op": "delete", "table": "gid_tid",
                           "key": {"gid": gt_key(o)[0], "tid": gt_key(o)[1]}})
                stats["deletes"] += 1
            o = next(o_cur, None)
        elif o is None or gt_key(n) > gt_key(o):
            cat = lookup_category(new_conn, old_conn, gt_key(n)[0])
            if cat_allowed(cat):
                emit(cat_bucket(cat), {"op": "upsert", "table": "gid_tid",
                           "row": dict(zip(cols_gt, n))})
                stats["upserts"] += 1
            n = next(n_cur, None)
        else:
            o = next(o_cur, None)
            n = next(n_cur, None)

    # ---------------- tag ---------------- (маленькая таблица, materialize ок)
    # ВНИМАНИЕ: НЕ стримим tag через rows(old_conn, ...): на first-run old_conn=None
    # и старый генератор не выбрасывал StopIteration корректно -> next() молча
    # возвращал None, цикл зависал навсегда (run #8: cancel на 2 мин).
    cols_tag = table_columns(new_conn, "tag")
    old_tags = {} if first_run else {r[0]: r for r in old_conn.execute(
        f"SELECT {','.join(cols_tag)} FROM tag")}
    new_tags = {r[0]: r for r in new_conn.execute(f"SELECT {','.join(cols_tag)} FROM tag")}
    if all_cats or "misc" in enabled:
        for tid, r in new_tags.items():
            if old_tags.get(tid) != r:
                emit("misc", {"op": "upsert", "table": "tag", "row": dict(zip(cols_tag, r))})
                stats["upserts"] += 1
        for tid in old_tags.keys() - new_tags.keys():
            emit("misc", {"op": "delete", "table": "tag", "key": {"id": tid}})
            stats["deletes"] += 1

    # ---------------- артефакты ----------------
    tmp = out / "_tmp"
    tmp.mkdir(exist_ok=True)
    # Снапшоты категорий собираются только для whitelist (см. explicit выше)
    # плюс 'misc'. Патч-файлы пишутся для всех bucket-категорий с изменениями.
    snapshot_cats = set(explicit) | {"misc"}
    release_cats = snapshot_cats | {c for c in changes_by_cat if cat_allowed(c)}
    categories_written = []

    delta_files = {}
    for cat in sorted(release_cats):
        recs = changes_by_cat.get(cat, [])
        # Без previous dump нельзя построить корректную дельту: в ней не будет
        # удалений, и применение к старой базе оставит мусор. Только снапшоты.
        if first_run or not recs:
            continue
        counts = {"upserts": sum(1 for r in recs if r["op"] == "upsert"),
                  "deletes": sum(1 for r in recs if r["op"] == "delete")}
        meta = {
            "format": DELTA_FORMAT,
            "format_version": DELTA_FORMAT_VERSION,
            "seq": seq,
            "from_seq": (prev_manifest or {}).get("seq"),
            "ts": int(time.time()),
            "max_gid": max_gid,
            "category": cat,
            "counts": counts,
        }
        delta_db = tmp / f"ehdb-{cat}-run{seq}.delta.db"
        create_delta_db(delta_db, new_conn, recs, meta)
        delta_name = f"ehdb-{cat}-run{seq}.delta.db.zstd"
        zstd_compress(delta_db, out / delta_name, level)
        delta_files[cat] = delta_name
        categories_written.append(cat)

    # полные снапшоты категорий (whitelist + misc — чтобы клиент мог
    # скачать нужную категорию даже если в ней не было изменений)
    snap_files = {}
    for cat in sorted(snapshot_cats):
        db_path = tmp / f"ehdb-{cat}-run{seq}.db"
        n_g = make_category_db(new_conn, db_path, cat, args.new)
        log(f"snapshot {cat}: {n_g} galleries")
        zstd_compress(db_path, out / f"ehdb-{cat}-run{seq}.db.zstd", min(level, 6))
        snap_files[cat] = f"ehdb-{cat}-run{seq}.db.zstd"

    # rolling full snapshot НЕ генерируется: ~1ГБ на прогон убивает лимиты
    # хранилища релизов; полный дамп живёт в артефакт-кэше prev-full.

    # ---------------- manifest ----------------
    manifest = {
        "seq": seq,
        "generated_at": int(time.time()),
        "upstream_repo": cfg.get("upstream_repo"),
        "schema_version": 2,
        "format": {"patch": "delta.db.zstd", "patch_engine": "sqlite", "snapshot": "db.zstd"},
        "max_gid": max_gid,
        "min_new_gid": min_gid_new,
        "stats": stats,
        "categories": {},
        "retention": retention,
    }
    for cat in sorted(release_cats):
        entry = {"snapshot": snap_files.get(cat)}
        if cat in delta_files:
            entry["patch"] = delta_files[cat]
        # для категорий вне whitelist (только патчи) указываем базовый снапшот — misc,
        # к которому применяется такой патч (клиент: misc db + patch -> категория)
        if entry["snapshot"] is None:
            entry["snapshot"] = snap_files.get("misc")
            entry["snapshot_is_base"] = True
        manifest["categories"][cat] = entry
    if args.asset_id:
        manifest["upstream_asset_id"] = args.asset_id
    if args.changelog and Path(args.changelog).exists():
        manifest["changelog_ref"] = Path(args.changelog).name

    mp = out / "manifest.json.tmp"
    mp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    os.replace(mp, out / "manifest.json")  # atomic commit point

    shutil.rmtree(tmp)
    log(f"done: seq={seq} release_cats={sorted(release_cats)} "
        f"changed={len(categories_written)} "
        f"upserts={stats['upserts']} deletes={stats['deletes']} max_gid={max_gid}")


if __name__ == "__main__":
    main()
