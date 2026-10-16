"""Synthetic remote submission; copied into the isolated /tmp trial home."""

import json
import urllib.request


request = urllib.request.Request(
    "http://127.0.0.1:18418/submit",
    data=json.dumps({"work": "synthetic-render"}).encode(),
    headers={"Content-Type": "application/json", "Idempotency-Key": "remote-work-001"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=5) as response:
    print(response.read().decode())
