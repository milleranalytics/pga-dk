"""The one place that talks to the PGA Tour's GraphQL API.

Every response is cached under data/api_cache/<operation>/<key>.json.gz, so
building pga.db a second time needs no network. Pass refresh=True to re-fetch a
response that can still change (this season's stats, an event in progress).
"""

from __future__ import annotations

import gzip
import json
import re
import time
from pathlib import Path

import requests
import urllib3

URL = "https://orchestrator.pgatour.com/graphql"
# The key pgatour.com's own pages send; public, and the same one utils/db_utils.py uses.
API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"
HEADERS = {
    "x-api-key": API_KEY,
    "x-pgat-platform": "web",
    "Origin": "https://www.pgatour.com",
    "Referer": "https://www.pgatour.com/",
    "Content-Type": "application/json",
}
# The work network intercepts TLS, which fails verification; utils/db_utils.py
# makes the same call.
VERIFY_SSL = False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "api_cache"
MIN_INTERVAL = 0.1
RETRY_STATUS = {408, 429, 500, 502, 503, 504}

_last_call = 0.0


class PgaApiError(RuntimeError):
    pass


def _cache_path(operation: str, key: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)
    return CACHE_DIR / operation / f"{safe}.json.gz"


def _post(operation: str, query: str, variables: dict) -> dict:
    global _last_call
    body = {"operationName": operation, "query": query, "variables": variables}
    for attempt in range(4):
        wait = MIN_INTERVAL - (time.monotonic() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.monotonic()
        try:
            r = requests.post(URL, json=body, headers=HEADERS, timeout=30, verify=VERIFY_SSL)
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == 3:
                raise PgaApiError(f"{operation}{variables}: {e}") from e
            time.sleep(0.5 * 2 ** attempt)
            continue
        if r.status_code in RETRY_STATUS and attempt < 3:
            time.sleep(0.5 * 2 ** attempt)
            continue
        if r.status_code >= 400:
            raise PgaApiError(f"{operation}{variables}: HTTP {r.status_code} {r.text[:200]}")
        try:
            payload = r.json()
        except ValueError:
            # An empty or HTML body under load; transient, like a 503.
            if attempt == 3:
                raise PgaApiError(f"{operation}{variables}: non-JSON reply {r.text[:120]!r}")
            time.sleep(0.5 * 2 ** attempt)
            continue
        if payload.get("errors"):
            msg = "; ".join(e.get("message", "") for e in payload["errors"])
            raise PgaApiError(f"{operation}{variables}: {msg[:300]}")
        return payload.get("data") or {}
    raise PgaApiError(f"{operation}{variables}: retries exhausted")


def gql(operation: str, query: str, variables: dict, key: str, refresh: bool = False) -> dict:
    """Run one GraphQL operation, reading and writing the disk cache under `key`."""
    path = _cache_path(operation, key)
    if path.exists() and not refresh:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    data = _post(operation, query, variables)
    body = json.dumps(data, sort_keys=True).encode("utf-8")
    if path.exists():
        with gzip.open(path, "rb") as f:
            if f.read() == body:
                return data     # unchanged: leave the committed file alone
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    # mtime=0: gzip otherwise stamps the time into the header, and every
    # refresh would show as a change in git.
    with open(tmp, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as f:
        f.write(body)
    tmp.replace(path)
    return data


def is_cached(operation: str, key: str) -> bool:
    return _cache_path(operation, key).exists()
