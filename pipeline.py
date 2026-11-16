#!/usr/bin/env python3
"""Satellite crop tiles batch pipeline (no HTTP service).

1. load tiles into the `tiles` table from N parallel writer threads (computed columns score each tile)
2. report timings and the health mix
3. export to Parquet, Apache Iceberg (SQLite-backed catalog) and LanceDB, then read each back

Usage:
    python pipeline.py                                   # local catalog 'crops'
    python pipeline.py --writers 4 --batch 6
    python pipeline.py --root pxt://<org>:<db>/crops     # cloud database
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path

import pixeltable as pxt

import udfs  # noqa: F401  (register the UDF module path)

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'out'
LOG = ROOT / 'logs' / 'run.jsonl'


def log(event: str, **payload) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    rec = {'event': event, 'ts': datetime.now().isoformat(timespec='seconds'), **payload}
    with LOG.open('a') as f:
        f.write(json.dumps(rec, default=str) + '\n')
    print(event, json.dumps(payload, default=str)[:600], flush=True)


def load_rows() -> list[dict]:
    meta = json.loads((ROOT / 'data' / 'tiles.json').read_text())
    return [
        {
            'tile_id': m['tile_id'],
            'field_id': m['field_id'],
            'region': m['region'],
            'captured_on': date.fromisoformat(m['captured_on']),
            'image': str(ROOT / m['path']),
        }
        for m in meta
    ]


def insert_batch(table_path: str, rows: list[dict]) -> tuple[int, int]:
    t = pxt.get_table(table_path)
    st = t.insert(rows, on_error='ignore')
    return st.num_rows, st.num_excs


def parallel_load(root: str, writers: int, batch: int) -> None:
    rows = load_rows()
    existing = {r['tile_id'] for r in pxt.get_table(f'{root}/tiles').select(pxt.get_table(f'{root}/tiles').tile_id).collect()}
    rows = [r for r in rows if r['tile_id'] not in existing]  # idempotent re-runs
    chunks = [rows[i : i + batch] for i in range(0, len(rows), batch)]
    t0 = time.perf_counter()
    inserted = errors = 0
    with ThreadPoolExecutor(max_workers=writers) as ex:
        for n, e in ex.map(lambda c: insert_batch(f'{root}/tiles', c), chunks):
            inserted += n
            errors += e
    log('load_done', writers=writers, batch=batch, chunks=len(chunks), rows=inserted, errors=errors,
        seconds=round(time.perf_counter() - t0, 2))


def export_all(root: str) -> None:
    t = pxt.get_table(f'{root}/tiles')
    flat = t.select(t.tile_id, t.field_id, t.region, t.captured_on, t.width, t.height, t.green, t.health)
    OUT.mkdir(exist_ok=True)

    t0 = time.perf_counter()
    pq = OUT / 'tiles_parquet'
    pq_img = OUT / 'tiles_parquet_images'
    for d in (pq, pq_img):
        shutil.rmtree(d, ignore_errors=True)  # export_parquet has no if_exists: re-runs need a clean target
    pxt.io.export_parquet(flat, pq)
    pxt.io.export_parquet(t.select(t.tile_id, t.image), pq_img, inline_images=True)  # images as bytes
    log('export_parquet', seconds=round(time.perf_counter() - t0, 2), files=len(list(pq.glob('*'))))

    t0 = time.perf_counter()
    from pyiceberg.catalog.sql import SqlCatalog

    wh = OUT / 'iceberg'
    wh.mkdir(exist_ok=True)
    cat = SqlCatalog('local', uri=f'sqlite:///{wh}/catalog.db', warehouse=f'file://{wh}')
    try:
        cat.create_namespace('crops')
    except Exception:  # namespace already exists on re-runs
        pass
    pxt.io.export_iceberg(flat, cat, 'crops.tiles', if_exists='replace')
    ice = cat.load_table('crops.tiles').scan().to_arrow()
    log('export_iceberg', seconds=round(time.perf_counter() - t0, 2), rows=ice.num_rows, cols=ice.column_names)

    t0 = time.perf_counter()
    import lancedb

    pxt.io.export_lancedb(flat, OUT / 'lance', 'tiles', if_exists='overwrite')
    n = lancedb.connect(str(OUT / 'lance')).open_table('tiles').count_rows()
    log('export_lancedb', seconds=round(time.perf_counter() - t0, 2), rows=n)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='crops', help='catalog directory (local) or pxt://<your-org>:<your-db>/dir (cloud)')
    ap.add_argument('--writers', type=int, default=4)
    ap.add_argument('--batch', type=int, default=6)
    ap.add_argument('--no-export', action='store_true')
    a = ap.parse_args()

    t = pxt.get_table(f'{a.root}/tiles')
    parallel_load(a.root, a.writers, a.batch)
    mix = {r['health']: r['n'] for r in t.group_by(t.health).select(t.health, n=pxt.functions.count(t.tile_id)).collect()}
    log('health_mix', mix=mix, total=t.count())
    if not a.no_export:
        export_all(a.root)


if __name__ == '__main__':
    main()
