"""Tiny client for the arbitration delivery fixtures."""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class UncertainDelivery(Exception):
    """The receiver may have committed; inspect or preserve ambiguity."""


def request(url: str, method: str, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, method=method, data=data,
                                 headers={"Authorization": "Bearer fixture-token",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode(errors='replace')}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise UncertainDelivery(str(error)) from error


def release(base_url: str, command: dict):
    return request(base_url.rstrip("/") + "/release", "POST", command)


def receipt(base_url: str, release_id: str):
    route = "/receipts/" + urllib.parse.quote(release_id, safe="")
    return request(base_url.rstrip("/") + route, "GET")


def opaque_submit(base_url: str, command: dict):
    return request(base_url.rstrip("/") + "/submit", "POST", command)
