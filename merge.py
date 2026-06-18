#!/usr/bin/env python3
"""
Merge sharded validator outputs into the final validated.{json,m3u,txt}.

Each parallel CI runner writes a partial result file (shard-<index>.json) via
validator.write_shard(). This script reads every shard partial, concatenates and
de-duplicates the surviving channels by stream URL, aggregates the per-shard meta
(parsed/live counts sum across shards), and writes the final outputs by reusing
validator.write_outputs().

Config via environment variables:
  SHARD_DIR   directory to scan for shard-*.json (default: OUTPUT_DIR)
  OUTPUT_DIR  where the final validated.* files are written (default: output)
"""

import glob
import json
import os
import sys
from datetime import datetime, timezone

import validator


def log(msg):
    print(msg, flush=True)


def find_shard_files(shard_dir):
    # Shards may be flattened into shard_dir or nested under per-artifact
    # subdirectories (GitHub download-artifact default layout). Match both.
    patterns = [
        os.path.join(shard_dir, 'shard-*.json'),
        os.path.join(shard_dir, '**', 'shard-*.json'),
    ]
    found = set()
    for p in patterns:
        found.update(glob.glob(p, recursive=True))
    return sorted(found)


def main():
    shard_dir = os.environ.get('SHARD_DIR', validator.OUTPUT_DIR)
    files = find_shard_files(shard_dir)
    log(f'=== Merge shard partials ===')
    log(f'Scanning {shard_dir} -> {len(files)} shard file(s).')
    if not files:
        log('! No shard files found. Writing empty outputs.')
        validator.write_outputs([], {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'sources': 0, 'totalParsed': 0, 'live': 0,
        })
        return 0

    merged = []
    seen = set()
    sources = 0
    total_parsed = 0
    live = 0
    generated_at = None

    for fp in files:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            log(f'  ! skipping {fp}: {e}')
            continue
        meta = data.get('meta', {}) or {}
        sources = max(sources, int(meta.get('sources', 0) or 0))
        total_parsed += int(meta.get('totalParsed', 0) or 0)
        live += int(meta.get('live', 0) or 0)
        generated_at = generated_at or meta.get('generatedAt')

        added = 0
        for ch in data.get('channels', []) or []:
            u = ch.get('url')
            if not u or u in seen:
                continue
            seen.add(u)
            merged.append(ch)
            added += 1
        log(f'  + {os.path.basename(fp)} -> {added} channels')

    meta = {
        'generatedAt': generated_at or datetime.now(timezone.utc).isoformat(),
        'sources': sources,
        'totalParsed': total_parsed,
        'live': live,
    }
    validator.write_outputs(merged, meta)
    log('=== Merge summary ===')
    log(f'  shards        : {len(files)}')
    log(f'  parsed (all)  : {total_parsed}')
    log(f'  live  (all)   : {live}')
    log(f'  validated     : {len(merged)}')
    log(f'  outputs       : {validator.OUTPUT_DIR}/validated.(json|m3u|txt)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
