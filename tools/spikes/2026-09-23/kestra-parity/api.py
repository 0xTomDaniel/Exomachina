"""Generic native Kestra v2 API driver; no candidate graph cases in this file."""
from __future__ import annotations

import argparse
import base64
import json
import netrc
import time
import urllib.error
import urllib.request
from pathlib import Path


class KestraAPI:
    def __init__(self, port: int, netrc_file: Path):
        self.base = f"http://127.0.0.1:{port}/api/v1/main"
        user, _, password = netrc.netrc(str(netrc_file)).authenticators("127.0.0.1")
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def call(self, method: str, path: str, data: bytes | None = None,
             content_type: str | None = None) -> dict:
        headers = {"Authorization": self.auth, "Accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                code, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, raw = error.code, error.read()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"raw": raw.decode(errors="replace")}
        return {"status": code, "body": body}

    @staticmethod
    def multipart(fields: dict[str, str] | None = None) -> tuple[bytes, str]:
        boundary = "exo-kestra-parity"
        parts = []
        for key, value in (fields or {}).items():
            parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n')
        parts.append(f"--{boundary}--\r\n")
        return "".join(parts).encode(), f"multipart/form-data; boundary={boundary}"

    def publish(self, file: Path, update: bool = False, namespace: str = "", flow_id: str = "") -> dict:
        path = f"/flows/{namespace}/{flow_id}" if update else "/flows"
        return self.call("PUT" if update else "POST", path, file.read_bytes(), "application/x-yaml")

    def execute(self, namespace: str, flow_id: str, fields: dict[str, str], revision: int | None = None) -> dict:
        body, kind = self.multipart(fields)
        path = f"/executions/{namespace}/{flow_id}"
        if revision is not None:
            path += f"?revision={revision}"
        return self.call("POST", path, body, kind)

    def resume(self, execution_id: str, fields: dict[str, str]) -> dict:
        body, kind = self.multipart(fields)
        return self.call("POST", f"/executions/{execution_id}/actions/resume", body, kind)

    def execution(self, execution_id: str) -> dict:
        return self.call("GET", "/executions/" + execution_id)

    def wait(self, execution_id: str, target: str, timeout: int = 90) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.execution(execution_id)
            if result["status"] != 200:
                return result
            state = result["body"]["state"]["current"]
            if state == target or state in {"FAILED", "KILLED", "CANCELLED"}:
                return result
            time.sleep(.25)
        return {"status": 408, "body": {"error": "wait timeout", "last": result["body"]}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--netrc", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    publication = sub.add_parser("publish")
    publication.add_argument("file", type=Path)
    publication.add_argument("--update", action="store_true")
    publication.add_argument("--namespace", default="")
    publication.add_argument("--flow-id", default="")
    validation = sub.add_parser("validate")
    validation.add_argument("file", type=Path)
    execution = sub.add_parser("execute")
    execution.add_argument("namespace")
    execution.add_argument("flow_id")
    execution.add_argument("--revision", type=int)
    execution.add_argument("--input", action="append", default=[])
    resume = sub.add_parser("resume")
    resume.add_argument("execution_id")
    resume.add_argument("--input", action="append", default=[])
    for name in ("get", "wait"):
        command = sub.add_parser(name)
        command.add_argument("execution_id")
        if name == "wait":
            command.add_argument("state")
    flow = sub.add_parser("flow")
    flow.add_argument("namespace")
    flow.add_argument("flow_id")
    flow.add_argument("--revision", type=int)
    evaluate = sub.add_parser("eval")
    evaluate.add_argument("execution_id")
    evaluate.add_argument("expression")
    listing = sub.add_parser("list")
    listing.add_argument("namespace")
    listing.add_argument("flow_id")
    args = parser.parse_args()
    client = KestraAPI(args.port, args.netrc)
    if args.command == "publish":
        result = client.publish(args.file, args.update, args.namespace, args.flow_id)
    elif args.command == "validate":
        result = client.call("POST", "/flows/validate", args.file.read_bytes(), "application/x-yaml")
    elif args.command == "execute":
        result = client.execute(args.namespace, args.flow_id, dict(item.split("=", 1) for item in args.input), args.revision)
    elif args.command == "resume":
        result = client.resume(args.execution_id, dict(item.split("=", 1) for item in args.input))
    elif args.command == "get":
        result = client.execution(args.execution_id)
    elif args.command == "wait":
        result = client.wait(args.execution_id, args.state)
    elif args.command == "flow":
        query = "" if args.revision is None else f"?revision={args.revision}"
        result = client.call("GET", f"/flows/{args.namespace}/{args.flow_id}{query}")
    elif args.command == "eval":
        result = client.call("POST", f"/executions/{args.execution_id}/actions/eval",
                             args.expression.encode(), "text/plain")
    elif args.command == "list":
        result = client.call("GET", f"/executions?namespace={args.namespace}&flowId={args.flow_id}")
    else:
        raise AssertionError(args.command)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    body = result["body"] if isinstance(result["body"], dict) else {}
    print(json.dumps({"status": result["status"], "id": body.get("id"),
                      "revision": body.get("revision", body.get("flowRevision")),
                      "state": body.get("state", {}).get("current") if isinstance(body.get("state"), dict) else None,
                      "out": str(args.out)}))
    if result["status"] >= 400:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
