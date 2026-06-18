# Auto M3U Validator

A headless, login-free Python port of the `di` Node M3U scanner. It reads M3U
playlist URLs from a text file, validates every channel in two stages, and
writes a clean list of **pure working channels** as JSON, M3U and TXT. A GitHub
Actions cron runs it automatically every 3 hours and commits the refreshed
outputs back to the repo.

## How it works

1. **Sources** — reads playlist URLs from [`sources.txt`](sources.txt), one per
   line (`#` lines are comments).
2. **Fetch + parse** — downloads every playlist and parses `#EXTINF` metadata
   (`tvg-id`, `tvg-name`, `tvg-logo`, `group-title`), combining and
   de-duplicating channels by stream URL.
3. **Stage 1 — dead check** — `HEAD`/`GET` probe per channel (HLS master →
   variant → segment aware). Dead channels and non-previewable `.ts`/`.mpd`
   streams are dropped. Only **live** channels continue.
4. **Stage 2 — deep auto-test** — the server-side equivalent of the browser
   `hls.js` "Auto Test All". It walks the HLS master → variant → media playlist
   and actually downloads a real media segment (and the AES key when the stream
   is encrypted), verifying genuine media bytes come back rather than an HTML
   error page. Anything that yields a **`Stream error: ...`** is filtered out.
5. **Stage 3 (optional) — ffmpeg decode probe** — when `FFPROBE_TEST=1` and
   `ffmpeg` is installed, each surviving channel is opened by ffmpeg and a few
   seconds are actually decoded. This is the strictest check: it drops streams
   that deliver bytes but won't truly play (bad codec, stalled feed, partial
   stream). It increases confidence, never the channel count.
6. **Outputs** — writes the surviving channels to:
   - `output/validated.json`
   - `output/validated.m3u`
   - `output/validated.txt`

## Run locally

```bash
pip install -r requirements.txt

# Add your playlist URLs to sources.txt first, then:
python validator.py
```

## Configuration (environment variables)

| Variable       | Default       | Description                                  |
|----------------|---------------|----------------------------------------------|
| `SOURCES_FILE` | `sources.txt` | Path to the playlist-URL list                |
| `OUTPUT_DIR`   | `output`      | Output directory                             |
| `CONCURRENCY`  | `16`          | Parallel workers for the dead check          |
| `TEST_WORKERS` | `6`           | Parallel workers for the deep auto-test      |
| `DEAD_TIMEOUT` | `9000`        | Per-request timeout (ms), stage 1            |
| `TEST_TIMEOUT` | `12000`       | Per-request timeout (ms), stage 2            |
| `DEEP_TEST`    | `1`           | Set `0` to run the dead check only           |
| `FFPROBE_TEST` | `0`           | Set `1` for Stage 3: ffmpeg actually decodes a few seconds (needs ffmpeg on PATH) |
| `FFPROBE_WORKERS` | `4`        | Parallel ffmpeg probes (Stage 3)             |
| `FFPROBE_SECS` | `4`           | Seconds of media ffmpeg decodes per channel  |
| `FFPROBE_TIMEOUT` | `15000`    | Per-probe I/O timeout (ms), Stage 3          |
| `VERIFY_SSL`   | `1`           | Set `0` to disable TLS verification          |
| `MAX_CHANNELS` | `0`           | Cap total channels (`0` = no cap)            |
| `IGNORE_GROUPS`| `Promo`       | Comma-separated group-titles to drop entirely|
| `IGNORE_NAMES` | `FalconCast`  | Comma-separated channel names to drop (substring, case-insensitive) |

## Automation

[`.github/workflows/validate.yml`](.github/workflows/validate.yml) runs on a
cron every 3 hours (and on manual dispatch / pushes to `sources.txt`). It
installs dependencies, runs `validator.py`, and commits any changes under
`output/` back to `main`.

To change the cadence, edit the `cron:` line (e.g. `0 */2 * * *` for every 2
hours).

## Files

| File             | Purpose                                              |
|------------------|------------------------------------------------------|
| `validator.py`   | Main pipeline (sources → stages → outputs)           |
| `checker.py`     | Dead check + deep auto-test (Stream Error detection) |
| `m3u.py`         | M3U parser / serializer                              |
| `sources.txt`    | Your playlist URLs (one per line)                    |
| `output/`        | Generated `validated.{json,m3u,txt}`                 |
