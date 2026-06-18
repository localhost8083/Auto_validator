"""
M3U parser / serializer.

A faithful Python port of the Node `lib/m3u.js`. Parses `#EXTM3U` + `#EXTINF`
lines (with tvg-id, tvg-name, tvg-logo, group-title and arbitrary extra
attributes) and the URL line that follows. Preserves group order and any
unknown directives (#EXTGRP, #EXTVLCOPT, #KODIPROP, ...) for faithful
re-emission.
"""

import re
from typing import Dict, List

_ATTR_RE = re.compile(r'([a-zA-Z0-9_-]+)="([^"]*)"')


def parse_attributes(attr_str: str) -> Dict[str, str]:
    """Parse a `#EXTINF` attribute string of key="value" pairs into a dict."""
    attrs: Dict[str, str] = {}
    if not attr_str:
        return attrs
    for m in _ATTR_RE.finditer(attr_str):
        attrs[m.group(1)] = m.group(2)
    return attrs


def parse_extinf(line: str) -> Dict:
    """Parse an `#EXTINF` line into {duration, attrs, name}."""
    body = re.sub(r'^#EXTINF:', '', line, flags=re.IGNORECASE)
    # Split on the first comma separating the header from the display name.
    comma_idx = body.find(',')
    header = body
    name = ''
    if comma_idx != -1:
        header = body[:comma_idx]
        name = body[comma_idx + 1:].strip()
    # header = "<duration> [attrs]"
    parts = header.strip().split()
    duration = -1
    if parts:
        try:
            duration = int(parts.pop(0))
        except ValueError:
            duration = -1
            # The first token was not a number; treat the whole header as attrs.
            parts = header.strip().split()
    attr_str = ' '.join(parts)
    attrs = parse_attributes(attr_str)
    return {'duration': duration, 'attrs': attrs, 'name': name}


def parse(text: str) -> List[Dict]:
    """
    Parse full M3U text into a list of channel dicts. Each channel:
        {name, url, attrs, duration, group, logo, tvgId, tvgName, extras: []}
    `extras` is the list of preceding non-EXTINF directive lines.
    """
    if not text:
        return []
    lines = re.split(r'\r?\n', text)
    channels: List[Dict] = []
    pending_extras: List[str] = []
    current = None
    saw_header = False

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if not saw_header and re.match(r'^#EXTM3U', line, re.IGNORECASE):
            saw_header = True
            continue

        if line.startswith('#EXTINF:'):
            parsed = parse_extinf(line)
            attrs = parsed['attrs']
            current = {
                'name': parsed['name'],
                'duration': parsed['duration'],
                'attrs': attrs,
                'tvgId': attrs.get('tvg-id', ''),
                'tvgName': attrs.get('tvg-name', ''),
                'logo': attrs.get('tvg-logo', ''),
                'group': attrs.get('group-title', 'Uncategorized'),
                'url': '',
                'extras': list(pending_extras),
            }
            pending_extras = []
        elif line.startswith('#EXTGRP:'):
            g = re.sub(r'^#EXTGRP:', '', line, flags=re.IGNORECASE).strip()
            if current and not current['attrs'].get('group-title'):
                current['group'] = g
                current['attrs']['group-title'] = g
            pending_extras.append(line)
        elif line.startswith('#'):
            pending_extras.append(line)
        else:
            # URL line.
            if current:
                current['url'] = line
                channels.append(current)
                current = None
            else:
                channels.append({
                    'name': 'Unnamed',
                    'duration': -1,
                    'attrs': {},
                    'tvgId': '',
                    'tvgName': '',
                    'logo': '',
                    'group': 'Uncategorized',
                    'url': line,
                    'extras': list(pending_extras),
                })
                pending_extras = []
    return channels


def esc_attr(v) -> str:
    """Escape an attribute value for re-emission (strip double quotes)."""
    return str('' if v is None else v).replace('"', '')


def serialize(channels: List[Dict]) -> str:
    """Serialize a list of channel dicts back to M3U text."""
    out = ['#EXTM3U']
    for c in channels:
        if not c or not c.get('url'):
            continue
        for e in c.get('extras', []) or []:
            out.append(e)
        attrs = c.get('attrs', {}) or {}
        attr_str = ' '.join(f'{k}="{esc_attr(v)}"' for k, v in attrs.items())
        duration = c.get('duration')
        duration = -1 if duration is None else duration
        header = f'{duration} {attr_str}' if attr_str else f'{duration}'
        name = c.get('name') or attrs.get('tvg-name') or 'Unnamed'
        out.append(f'#EXTINF:{header},{name}')
        out.append(c['url'])
    return '\n'.join(out) + '\n'
