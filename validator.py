#!/usr/bin/env python3
"""
Auto M3U validator (headless port of the Node `di` scanner).

Pipeline:
  1. Read one-or-more M3U playlist URLs from a .txt file (default: sources.txt),
     one URL per line. Lines starting with '#' are treated as comments.
  2. Fetch + parse every playlist into a combined, de-duplicated channel list.
  3. STAGE 1 - dead check: drop dead and non-previewable (.ts/.mpd) channels.
  4. STAGE 2 - deep auto-test: download a real media segment for each surviving
     channel; drop anything that yields a "Stream error".
  5. Write the pure working channel list to output/validated.{json,m3u,txt}.

No login, no server, no browser - safe to run on a GitHub Actions cron.

Config via environment variables (all optional):
  SOURCES_FILE   path to the playlist-URL list           (default sources.txt)
  OUTPUT_DIR     directory for outputs                    (default output)
  CONCURRENCY    parallel workers for stage 1             (default 16)
  TEST_WORKERS   parallel workers for stage 2             (default 6)
  DEAD_TIMEOUT   per-request timeout (ms) for stage 1     (default 9000)
  TEST_TIMEOUT   per-request timeout (ms) for stage 2     (default 12000)
  DEEP_TEST      "0" to skip stage 2 (dead-check only)    (default 1)
  VERIFY_SSL     "0" to disable TLS verification          (default 1)
  MAX_CHANNELS   cap total channels (0 = no cap)          (default 0)
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter

import m3u
from checker import check_stream, deep_validate, ffmpeg_available, ffmpeg_validate, headers

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
ROOT = os.path.dirname(os.path.abspath(__file__))


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def env_list(name, default):
    """Parse a comma-separated env var into a list, or return the default list."""
    raw = os.environ.get(name)
    if raw is None:
        return list(default)
    return [s.strip() for s in raw.split(',') if s.strip()]


SOURCES_FILE = os.environ.get('SOURCES_FILE', os.path.join(ROOT, 'sources.txt'))
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', os.path.join(ROOT, 'output'))
CONCURRENCY = max(1, env_int('CONCURRENCY', 16))
TEST_WORKERS = max(1, env_int('TEST_WORKERS', 6))
DEAD_TIMEOUT = env_int('DEAD_TIMEOUT', 9000)
TEST_TIMEOUT = env_int('TEST_TIMEOUT', 12000)
DEEP_TEST = os.environ.get('DEEP_TEST', '1') != '0'

# Optional STAGE 3: ffmpeg actually decodes a few seconds of each surviving
# channel. Strictest check; runs only if FFPROBE_TEST != 0 and ffmpeg is on PATH.
FFPROBE_TEST = os.environ.get('FFPROBE_TEST', '0') != '0'
FFPROBE_WORKERS = max(1, env_int('FFPROBE_WORKERS', 4))
FFPROBE_SECS = max(1, env_int('FFPROBE_SECS', 4))
FFPROBE_TIMEOUT = env_int('FFPROBE_TIMEOUT', 15000)
VERIFY_SSL = os.environ.get('VERIFY_SSL', '1') != '0'
MAX_CHANNELS = env_int('MAX_CHANNELS', 0)

# Sharding: split the (deduplicated, filtered) channel list across N parallel
# CI runners. Each shard validates a disjoint, interleaved slice and writes a
# partial result file (output/shard-<index>.json) that merge.py recombines into
# the final outputs. SHARD_COUNT=1 (default) = single-machine behaviour as before.
SHARD_COUNT = max(1, env_int('SHARD_COUNT', 1))
SHARD_INDEX = max(0, env_int('SHARD_INDEX', 0))

# Channels whose group-title or name match these are dropped entirely BEFORE any
# scan/validation/output. Comma-separated, case-insensitive. Group match is per
# token (group-title may be like "News;Public"); name match is substring (so
# "FalconCast" also catches "FalconCast (1080p)").
IGNORE_GROUPS = env_list('IGNORE_GROUPS', ['Promo'])
IGNORE_NAMES = env_list('IGNORE_NAMES', ['FalconCast'])


def is_ignored(ch):
    """True if a channel matches the group/name blocklist."""
    group = ch.get('group') or ''
    group_full = group.strip().lower()
    group_tokens = [t.strip().lower() for t in re.split(r'[;,]', group) if t.strip()]
    for g in IGNORE_GROUPS:
        gl = g.strip().lower()
        if gl and (gl == group_full or gl in group_tokens):
            return True
    name = (ch.get('name') or '').strip().lower()
    for n in IGNORE_NAMES:
        nl = n.strip().lower()
        if nl and nl in name:
            return True
    return False


def log(msg):
    print(msg, flush=True)


def make_session():
    s = requests.Session()
    s.verify = VERIFY_SSL
    adapter = HTTPAdapter(pool_connections=CONCURRENCY * 2, pool_maxsize=CONCURRENCY * 2)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    return s


# --------------------------------------------------------------------------- #
# Input
# --------------------------------------------------------------------------- #
def read_sources(path):
    if not os.path.exists(path):
        log(f'! sources file not found: {path}')
        return []
    urls = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.lower().startswith(('http://', 'https://')):
                urls.append(line)
    return urls


def fetch_playlists(session, urls):
    """Fetch + parse every playlist, combine and de-duplicate channels by URL."""
    channels = []
    seen = set()
    for u in urls:
        try:
            r = session.get(u, headers=headers(), timeout=30, allow_redirects=True)
            if not (200 <= r.status_code < 300):
                log(f'  ! {u} -> HTTP {r.status_code}')
                continue
            parsed = m3u.parse(r.text)
            added = 0
            for ch in parsed:
                if not ch.get('url'):
                    continue
                if not ch.get('source'):
                    ch['source'] = u
                key = ch['url']
                if key in seen:
                    continue
                seen.add(key)
                channels.append(ch)
                added += 1
            log(f'  + {u} -> {added} channels')
        except requests.RequestException as e:
            log(f'  ! {u} -> {str(e)[:120]}')
    return channels


# --------------------------------------------------------------------------- #
# Stages
# --------------------------------------------------------------------------- #
def run_pool(items, fn, workers, label):
    """Run fn(item) across a thread pool, logging incremental progress."""
    results = [None] * len(items)
    done = 0
    total = len(items)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fn, i, item): i for i, item in enumerate(items)}
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                results[i] = fut.result()
            except Exception as e:  # noqa: BLE001 - never let one channel kill the run
                results[i] = {'_error': str(e)}
            done += 1
            if done % 25 == 0 or done == total:
                log(f'  {label}: {done}/{total}')
    return results


def stage_dead_check(session, channels):
    def work(_i, ch):
        return check_stream(session, ch['url'], DEAD_TIMEOUT)

    results = run_pool(channels, work, CONCURRENCY, 'dead-check')
    live, dead, unplayable = [], 0, 0
    for ch, res in zip(channels, results):
        if res and res.get('live'):
            ch['_check'] = res
            live.append(ch)
        elif res and res.get('status') == 'unplayable':
            unplayable += 1
        else:
            dead += 1
    return live, dead, unplayable


def stage_auto_test(session, channels):
    def work(_i, ch):
        return deep_validate(session, ch['url'], TEST_TIMEOUT)

    results = run_pool(channels, work, TEST_WORKERS, 'auto-test')
    passed, failed = [], []
    for ch, res in zip(channels, results):
        if res and res.get('ok'):
            ch['_test'] = res
            passed.append(ch)
        else:
            ch['_test'] = res or {'ok': False, 'reason': 'Stream error: unknown'}
            failed.append(ch)
    return passed, failed


def stage_ffmpeg(channels):
    if not ffmpeg_available():
        log('  ! ffmpeg not found on PATH - skipping decode probe (no channels dropped).')
        return channels, []

    def work(_i, ch):
        return ffmpeg_validate(ch['url'], FFPROBE_TIMEOUT, FFPROBE_SECS)

    results = run_pool(channels, work, FFPROBE_WORKERS, 'ffmpeg')
    passed, failed = [], []
    for ch, res in zip(channels, results):
        if res and res.get('ok'):
            ch['_ffmpeg'] = res
            passed.append(ch)
        else:
            ch['_ffmpeg'] = res or {'ok': False, 'reason': 'ffmpeg: unknown'}
            failed.append(ch)
    return passed, failed


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def write_outputs(channels, meta):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # JSON (matches the Node export schema, enriched with validation metadata).
    json_channels = []
    for c in channels:
        chk = c.get('_check', {})
        json_channels.append({
            'name': c.get('name', ''),
            'group': c.get('group') or 'Uncategorized',
            'logo': c.get('logo', ''),
            'tvgId': c.get('tvgId', ''),
            'tvgName': c.get('tvgName', ''),
            'url': c['url'],
            'source': c.get('source', ''),
            'latencyMs': chk.get('ms', 0),
        })
    payload = {
        'generatedAt': meta['generatedAt'],
        'sources': meta['sources'],
        'totalParsed': meta['totalParsed'],
        'live': meta['live'],
        'validated': len(channels),
        'channels': json_channels,
    }
    with open(os.path.join(OUTPUT_DIR, 'validated.json'), 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # M3U (rebuild EXTINF with tvg attrs, like the Node exportM3u).
    m3u_channels = []
    for c in channels:
        attrs = {}
        if c.get('tvgId'):
            attrs['tvg-id'] = c['tvgId']
        attrs['tvg-name'] = c.get('name') or c.get('tvgName') or 'Unnamed'
        if c.get('logo'):
            attrs['tvg-logo'] = c['logo']
        attrs['group-title'] = c.get('group') or 'Uncategorized'
        m3u_channels.append({
            'name': c.get('name', 'Unnamed'),
            'url': c['url'],
            'duration': -1,
            'attrs': attrs,
            'extras': [],
        })
    with open(os.path.join(OUTPUT_DIR, 'validated.m3u'), 'w', encoding='utf-8') as f:
        f.write(m3u.serialize(m3u_channels))

    # TXT (Name | Logo URL | Stream URL), matching the Node exportTxt header.
    with open(os.path.join(OUTPUT_DIR, 'validated.txt'), 'w', encoding='utf-8') as f:
        f.write('Name | Logo URL | Stream URL\n')
        for c in channels:
            f.write(f"{c.get('name', '')} | {c.get('logo', '')} | {c['url']}\n")


def write_shard(channels, meta, index):
    """Write a single shard's surviving channels + per-shard meta as JSON.

    merge.py reads every shard-*.json and rebuilds the final validated outputs.
    The channel dicts keep their _check/_test/_ffmpeg metadata (all JSON-safe).
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f'shard-{index}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'meta': meta, 'channels': channels}, f, ensure_ascii=False)
    log(f'  wrote shard partial -> {path} ({len(channels)} channels)')


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    t0 = time.time()
    log('=== Auto M3U validator ===')
    urls = read_sources(SOURCES_FILE)
    if not urls:
        log('No source playlist URLs found. Add http(s) M3U links to '
            f'{os.path.basename(SOURCES_FILE)} (one per line).')
        # Still write empty outputs so the workflow has deterministic artifacts.
        write_outputs([], {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'sources': 0, 'totalParsed': 0, 'live': 0,
        })
        return 0

    session = make_session()

    log(f'Fetching {len(urls)} playlist source(s)...')
    channels = fetch_playlists(session, urls)

    if IGNORE_GROUPS or IGNORE_NAMES:
        before = len(channels)
        channels = [c for c in channels if not is_ignored(c)]
        removed = before - len(channels)
        if removed:
            log(f'Ignored {removed} channel(s) via blocklist '
                f'(groups={IGNORE_GROUPS}, names={IGNORE_NAMES}).')

    if MAX_CHANNELS and len(channels) > MAX_CHANNELS:
        log(f'Capping {len(channels)} -> {MAX_CHANNELS} channels (MAX_CHANNELS).')
        channels = channels[:MAX_CHANNELS]
    total_parsed = len(channels)
    log(f'Parsed {total_parsed} unique channels.')

    # Split into a disjoint, interleaved slice for this runner. Interleaving
    # (start::step) spreads same-source / similar-latency channels evenly across
    # shards so each runner finishes in roughly the same wall-clock time.
    if SHARD_COUNT > 1:
        channels = channels[SHARD_INDEX::SHARD_COUNT]
        log(f'Shard {SHARD_INDEX + 1}/{SHARD_COUNT}: validating '
            f'{len(channels)} of {total_parsed} channels.')
        total_parsed = len(channels)

    if total_parsed == 0:
        empty_meta = {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'sources': len(urls), 'totalParsed': 0, 'live': 0,
        }
        if SHARD_COUNT > 1:
            write_shard([], empty_meta, SHARD_INDEX)
        else:
            write_outputs([], empty_meta)
        log('Nothing to validate.')
        return 0

    log(f'STAGE 1 - dead check ({CONCURRENCY} workers)...')
    live, dead, unplayable = stage_dead_check(session, channels)
    log(f'  live={len(live)}  dead={dead}  unplayable(.ts/.mpd)={unplayable}')

    if DEEP_TEST and live:
        log(f'STAGE 2 - deep auto-test ({TEST_WORKERS} workers)...')
        validated, failed = stage_auto_test(session, live)
        log(f'  passed={len(validated)}  stream-errors-removed={len(failed)}')
    else:
        validated = live
        if not DEEP_TEST:
            log('STAGE 2 skipped (DEEP_TEST=0).')

    if FFPROBE_TEST and validated:
        log(f'STAGE 3 - ffmpeg decode probe ({FFPROBE_WORKERS} workers, {FFPROBE_SECS}s each)...')
        validated, ff_failed = stage_ffmpeg(validated)
        log(f'  decoded-ok={len(validated)}  ffmpeg-removed={len(ff_failed)}')
    elif FFPROBE_TEST:
        log('STAGE 3 skipped (nothing to probe).')

    meta = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'sources': len(urls),
        'totalParsed': total_parsed,
        'live': len(live),
    }
    if SHARD_COUNT > 1:
        write_shard(validated, meta, SHARD_INDEX)
    else:
        write_outputs(validated, meta)

    dt = time.time() - t0
    log('=== Summary ===')
    log(f'  sources       : {len(urls)}')
    log(f'  parsed        : {total_parsed}')
    log(f'  live          : {len(live)}')
    log(f'  validated     : {len(validated)}')
    log(f'  elapsed       : {dt:.1f}s')
    log(f'  outputs       : {OUTPUT_DIR}/validated.(json|m3u|txt)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
