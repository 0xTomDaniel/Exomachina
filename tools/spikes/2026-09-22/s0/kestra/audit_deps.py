from __future__ import annotations

import csv
import io
import re
import threading
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = 'https://repo.maven.apache.org/maven2'
_cache: dict[tuple[str, str, str], tuple[ET.Element | None, str, str]] = {}
_lock = threading.Lock()


def find_child(node: ET.Element, name: str) -> ET.Element | None:
    return next((e for e in node if e.tag.rsplit('}', 1)[-1] == name), None)


def child_text(node: ET.Element, name: str) -> str:
    e = find_child(node, name)
    return (e.text or '').strip() if e is not None else ''


def pom(group: str, artifact: str, version: str) -> tuple[ET.Element | None, str, str]:
    key = (group, artifact, version)
    with _lock:
        if key in _cache:
            return _cache[key]
    url = f'{BASE}/{group.replace(".", "/")}/{artifact}/{version}/{artifact}-{version}.pom'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Exomachina-bounded-S0-license-audit/1.0'})
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = resp.read(2_000_000)
        value = (ET.fromstring(data), url, 'ok')
    except urllib.error.HTTPError as e:
        value = (None, url, f'http-{e.code}')
    except Exception as e:
        value = (None, url, type(e).__name__)
    with _lock:
        _cache[key] = value
    return value


def direct_licenses(root: ET.Element) -> list[tuple[str, str]]:
    licenses = find_child(root, 'licenses')
    if licenses is None:
        return []
    return [(child_text(item, 'name'), child_text(item, 'url')) for item in licenses if item.tag.rsplit('}', 1)[-1] == 'license']


def resolve(row: dict[str, str]) -> dict[str, str]:
    group, artifact, version = row['group'], row['artifact'], row['version']
    visited = set()
    current = (group, artifact, version)
    results: list[str] = []
    source_urls: list[str] = []
    statuses: list[str] = []
    for depth in range(5):
        if current in visited:
            statuses.append('parent-cycle')
            break
        visited.add(current)
        root, url, status = pom(*current)
        source_urls.append(url)
        statuses.append(status)
        if root is None:
            break
        licenses = direct_licenses(root)
        if licenses:
            results = [f'{name} <{link}>' if link else name for name, link in licenses]
            break
        parent = find_child(root, 'parent')
        if parent is None:
            break
        nxt = (child_text(parent, 'groupId'), child_text(parent, 'artifactId'), child_text(parent, 'version'))
        if not all(nxt) or any('${' in part for part in nxt):
            statuses.append('unresolved-parent')
            break
        current = nxt
    return {
        'group': group, 'artifact': artifact, 'version': version,
        'classification': ('direct-pom' if len(source_urls) == 1 else 'parent-pom') if results else ('missing-pom' if 'http-404' in statuses else 'no-pom-license'),
        'license': '; '.join(results), 'source_pom': source_urls[-1] if results else '',
        'pom_chain': ' | '.join(source_urls), 'fetch_status': ' | '.join(statuses),
    }

with Path('dependency-inventory.csv').open() as f:
    unknown = [row for row in csv.DictReader(f) if row['declared license'] == 'unknown/not embedded']
with ThreadPoolExecutor(max_workers=8) as pool:
    rows = list(pool.map(resolve, unknown))
with Path('pom-license-audit.csv').open('w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)
from collections import Counter
print('audited', len(rows), 'components, fetched', len(_cache), 'unique POMs')
print('classifications', dict(Counter(r['classification'] for r in rows)))
for row in rows:
    if row['classification'] in ('missing-pom', 'no-pom-license'):
        print('UNRESOLVED', row['group'], row['artifact'], row['version'], row['fetch_status'])
for row in rows:
    if re.search(r'AGPL|SSPL|BUSINESS SOURCE|COMMONS CLAUSE|ORKES|NON.COMMERCIAL|GPL', row['license'], re.I):
        print('REVIEW', row['group'], row['artifact'], row['version'], row['license'])
