"""
Stream liveness checker + deep auto-test.

Two stages, mirroring the Node app:

STAGE 1 - dead check (port of `lib/stream-checker.js` `checkStream`):
  * `.ts` / `.mpd` -> 'unplayable' (dropped, not previewable), no network.
  * HEAD -> 2xx/3xx -> live.
  * HLS (.m3u8): GET playlist. Master -> recurse first variant. Media -> HEAD a
    segment. Reachable -> live.
  * Direct: GET with Range bytes=0-1, 2xx/3xx -> live.

STAGE 2 - deep auto-test (server-side equivalent of the browser hls.js test in
`public/app.js`). The browser actually plays each "live" channel and flags
`Stream error: <detail>` on fatal/network errors. We can't run a media engine
in CI, so we do the strongest server-side equivalent: walk master -> variant ->
media playlist and actually download a real media segment (and the AES key if
the stream is encrypted), verifying we receive genuine media bytes rather than
an HTML error page. Anything that fails is reported as a `Stream error: ...`
and filtered out, exactly like the browser does.

Public API:
  check_stream(session, url, timeout_ms)  -> dict(live,status,httpCode,ms,reason)
  deep_validate(session, url, timeout_ms) -> dict(ok,reason,ms)
"""

import re
import time
from urllib.parse import urljoin

import requests

DEFAULT_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'
)

# Max bytes pulled from a media segment when proving it actually delivers media.
SEGMENT_PROBE_BYTES = 64 * 1024


def headers(extra=None):
    h = {'User-Agent': DEFAULT_UA, 'Accept': '*/*'}
    if extra:
        h.update(extra)
    return h


def classify_url(url: str) -> str:
    """Classify a stream URL the same way the Node front-end does."""
    if re.search(r'\.m3u8(\?.*)?$', url, re.IGNORECASE):
        return 'hls'
    if re.search(r'\.ts(\?.*)?$', url, re.IGNORECASE):
        return 'ts'
    if re.search(r'\.mpd(\?.*)?$', url, re.IGNORECASE):
        return 'dash'
    if re.search(r'\.(mp4|m4v|webm|ogg|ogv|mkv|mov)(\?.*)?$', url, re.IGNORECASE):
        return 'video'
    return 'unknown'


def is_hls(url: str) -> bool:
    return bool(re.search(r'\.m3u8(\?.*)?$', url, re.IGNORECASE))


def _parse_hls_body(text: str):
    return [ln.strip() for ln in re.split(r'\r?\n', text) if ln.strip()]


def _looks_like_manifest(text: str) -> bool:
    t = text.lstrip('\ufeff').strip()
    return (
        t.startswith('#EXTM3U')
        or t.startswith('#EXT-X-')
        or t.startswith('#EXTINF')
    )


def _looks_like_html(chunk: bytes) -> bool:
    head = chunk[:512].lstrip().lower()
    return head.startswith(b'<!doctype') or head.startswith(b'<html') or head.startswith(b'<')


# --------------------------------------------------------------------------- #
# STAGE 1 - dead check
# --------------------------------------------------------------------------- #
def check_stream(session: requests.Session, url: str, timeout_ms: int = 9000) -> dict:
    """Quick liveness probe. Never raises. Returns dict(live,status,httpCode,ms,reason)."""
    timeout = max(2.0, timeout_ms / 1000.0)
    start = time.time()
    result = {'live': False, 'status': 'unknown', 'httpCode': 0, 'ms': 0, 'reason': ''}

    def done():
        result['ms'] = int((time.time() - start) * 1000)
        return result

    if not url or not re.match(r'^https?://', url, re.IGNORECASE):
        result['status'] = 'invalid'
        result['reason'] = 'Not an http(s) URL'
        return done()

    kind = classify_url(url)
    if kind in ('ts', 'dash'):
        result['status'] = 'unplayable'
        label = 'Direct .ts' if kind == 'ts' else 'DASH .mpd'
        result['reason'] = f'{label} stream - not previewable'
        return done()

    # 1) Quick HEAD.
    try:
        head = session.head(url, headers=headers(), timeout=timeout, allow_redirects=True)
        result['httpCode'] = head.status_code
        if 200 <= head.status_code < 400:
            result['live'] = True
            result['status'] = 'live'
            return done()
    except requests.RequestException:
        pass  # Many servers reject HEAD; fall through to GET probing.

    # 2) HLS path.
    if is_hls(url):
        try:
            res = session.get(url, headers=headers(), timeout=timeout, allow_redirects=True)
            result['httpCode'] = res.status_code
            if 200 <= res.status_code < 300:
                lines = _parse_hls_body(res.text)
                variant = next((l for l in lines if re.match(r'^#EXT-X-STREAM-INF:', l, re.IGNORECASE)), None)
                if variant:
                    vi = lines.index(variant)
                    nxt = lines[vi + 1] if vi + 1 < len(lines) else None
                    if nxt and not nxt.startswith('#'):
                        sub_url = urljoin(url, nxt)
                        sub = check_stream(session, sub_url, min(timeout_ms, 6000))
                        if sub['live']:
                            result['live'] = True
                            result['status'] = 'live'
                            result['reason'] = 'HLS master -> variant OK'
                            return done()
                        result['status'] = 'dead'
                        result['reason'] = 'HLS variant failed: ' + (sub['reason'] or sub['status'])
                        return done()
                # Media playlist -> HEAD a segment.
                seg = next((l for i, l in enumerate(lines) if i > 0 and not l.startswith('#')), None)
                if seg is not None:
                    seg_url = urljoin(url, seg)
                    try:
                        sh = session.head(seg_url, headers=headers(), timeout=min(timeout, 6.0), allow_redirects=True)
                        if 200 <= sh.status_code < 400:
                            result['live'] = True
                            result['status'] = 'live'
                            return done()
                        result['httpCode'] = sh.status_code
                        result['status'] = 'dead'
                        result['reason'] = f'Segment HEAD {sh.status_code}'
                        return done()
                    except requests.RequestException as e:
                        result['status'] = 'dead'
                        result['reason'] = 'Segment error: ' + str(e)[:120]
                        return done()
                # Reachable, no segments yet (live edge).
                result['live'] = True
                result['status'] = 'live'
                result['reason'] = 'HLS playlist reachable'
                return done()
            result['status'] = 'dead'
            result['reason'] = f'HLS GET {res.status_code}'
            return done()
        except requests.RequestException as e:
            result['status'] = 'dead'
            result['reason'] = 'HLS error: ' + str(e)[:120]
            return done()

    # 3) Direct stream: GET with a tiny Range.
    try:
        res = session.get(url, headers=headers({'Range': 'bytes=0-1'}),
                          timeout=timeout, allow_redirects=True, stream=True)
        result['httpCode'] = res.status_code
        if 200 <= res.status_code < 400:
            try:
                next(res.iter_content(2), b'')
            except Exception:
                pass
            finally:
                res.close()
            result['live'] = True
            result['status'] = 'live'
            return done()
        res.close()
        result['status'] = 'dead'
        result['reason'] = f'HTTP {res.status_code}'
        return done()
    except requests.RequestException as e:
        result['status'] = 'dead'
        result['reason'] = 'Error: ' + str(e)[:120]
        return done()


# --------------------------------------------------------------------------- #
# STAGE 2 - deep auto-test ("Stream error" detection)
# --------------------------------------------------------------------------- #
def _probe_segment(session, seg_url, timeout):
    """Download a slice of a media segment. Returns (ok, reason)."""
    try:
        r = session.get(seg_url, headers=headers({'Range': f'bytes=0-{SEGMENT_PROBE_BYTES - 1}'}),
                        timeout=timeout, allow_redirects=True, stream=True)
    except requests.RequestException as e:
        return False, 'Stream error: segment unreachable (' + str(e)[:80] + ')'
    try:
        if not (200 <= r.status_code < 400):
            return False, f'Stream error: segment HTTP {r.status_code}'
        chunk = b''
        for part in r.iter_content(8192):
            chunk += part
            if len(chunk) >= 4096:
                break
        if not chunk:
            return False, 'Stream error: empty segment body'
        if _looks_like_html(chunk):
            return False, 'Stream error: segment returned HTML (access denied / error page)'
        ctype = (r.headers.get('content-type') or '').lower()
        if 'text/html' in ctype or 'application/json' in ctype:
            return False, f'Stream error: bad segment content-type ({ctype})'
        # TS sync byte (0x47) is the strongest positive signal; otherwise accept
        # any non-HTML binary payload (fmp4/aac/etc).
        return True, 'segment delivered media bytes'
    finally:
        r.close()


def deep_validate(session: requests.Session, url: str, timeout_ms: int = 12000, depth: int = 0) -> dict:
    """
    Strong "does it actually play" test. Mirrors the browser auto-test: walks the
    HLS master -> variant -> media playlist and downloads a real segment (plus the
    AES key when encrypted). Returns dict(ok, reason, ms). Failures are reported as
    `Stream error: ...` so the caller can filter them, exactly like hls.js does.
    """
    timeout = max(3.0, timeout_ms / 1000.0)
    start = time.time()

    def out(ok, reason):
        return {'ok': ok, 'reason': reason, 'ms': int((time.time() - start) * 1000)}

    if depth > 3:
        return out(False, 'Stream error: variant nesting too deep')

    kind = classify_url(url)

    # Direct video (.mp4/.webm/...) or unknown: confirm it serves real media bytes.
    if kind in ('video', 'unknown'):
        ok, reason = _probe_segment(session, url, timeout)
        return out(ok, reason if ok else reason)

    if kind in ('ts', 'dash'):
        return out(False, 'Stream error: not browser-previewable (.ts/.mpd)')

    # HLS.
    try:
        res = session.get(url, headers=headers(), timeout=timeout, allow_redirects=True)
    except requests.RequestException as e:
        return out(False, 'Stream error: playlist unreachable (' + str(e)[:80] + ')')
    if not (200 <= res.status_code < 300):
        return out(False, f'Stream error: HLS GET {res.status_code}')
    body = res.text
    if not _looks_like_manifest(body):
        return out(False, 'Stream error: not an HLS manifest (HTML/error page)')

    lines = _parse_hls_body(body)

    # Master playlist -> recurse into the first variant.
    variant = next((l for l in lines if re.match(r'^#EXT-X-STREAM-INF:', l, re.IGNORECASE)), None)
    if variant:
        vi = lines.index(variant)
        nxt = lines[vi + 1] if vi + 1 < len(lines) else None
        if not nxt or nxt.startswith('#'):
            return out(False, 'Stream error: master playlist has no variant URL')
        sub = deep_validate(session, urljoin(url, nxt), timeout_ms, depth + 1)
        return out(sub['ok'], sub['reason'])

    # Media playlist: validate encryption key (if any), then a segment.
    key_line = next((l for l in lines if re.match(r'^#EXT-X-KEY:', l, re.IGNORECASE) and 'URI="' in l), None)
    if key_line:
        m = re.search(r'URI="([^"]+)"', key_line)
        if m and 'NONE' not in key_line.upper():
            key_url = urljoin(url, m.group(1))
            try:
                kr = session.get(key_url, headers=headers(), timeout=min(timeout, 6.0), allow_redirects=True)
                if not (200 <= kr.status_code < 400) or not kr.content:
                    return out(False, f'Stream error: AES key fetch failed ({kr.status_code})')
            except requests.RequestException as e:
                return out(False, 'Stream error: AES key unreachable (' + str(e)[:60] + ')')

    seg = next((l for i, l in enumerate(lines) if i > 0 and not l.startswith('#')), None)
    if seg is None:
        # Reachable live-edge playlist with no segments listed yet: accept.
        return out(True, 'HLS playlist reachable (live edge, no segment listed)')

    ok, reason = _probe_segment(session, urljoin(url, seg), timeout)
    return out(ok, reason)
