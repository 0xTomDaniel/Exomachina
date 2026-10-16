"""Small A2A 0.3.0 Director client for the arbitration probe."""
from __future__ import annotations

import argparse
import json
import urllib.request
from uuid import uuid4


def rpc(url: str, method: str, params: dict) -> dict:
    payload = {"jsonrpc": "2.0", "id": str(uuid4()), "method": method, "params": params}
    request = urllib.request.Request(url.rstrip("/") + "/", data=json.dumps(payload).encode(),
        headers={"Authorization": "Bearer fixture-token", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.load(response)
    if "error" in body:
        raise RuntimeError(body["error"])
    return body["result"]


def send(url: str, command: dict) -> dict:
    result = rpc(url, "message/send", {"message": {"role": "user", "messageId": str(uuid4()),
               "parts": [{"kind": "data", "data": command}]}})
    if result.get("kind") != "task":
        raise RuntimeError("Director rejected command: " + json.dumps(result, sort_keys=True))
    return result


def get(url: str, task_id: str) -> dict:
    return rpc(url, "tasks/get", {"id": task_id})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    sub = parser.add_subparsers(dest="op", required=True)
    send_parser = sub.add_parser("send")
    send_parser.add_argument("--command-json", required=True)
    get_parser = sub.add_parser("get")
    get_parser.add_argument("--task-id", required=True)
    args = parser.parse_args()
    value = send(args.url, json.loads(args.command_json)) if args.op == "send" else get(args.url, args.task_id)
    print(json.dumps(value, indent=2, sort_keys=True))
