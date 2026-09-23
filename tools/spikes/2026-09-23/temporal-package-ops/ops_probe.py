"""Ordinary package security, schema and 0/2/10 held A2A measurements."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun
from footprint_probe import sample
from probe import SERVICE_PORTS, bindings, director_send, director_task

PREFIX = Path(__file__).resolve().parent.parent
STATE = PREFIX / "state"
runtime.STATE = STATE


def sql(query: str, *, password: str | None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("PGPASSWORD", None)
    if password is not None:
        env["PGPASSWORD"] = password
    return subprocess.run([str(runtime.PG_BIN / "psql"), "-w", "-h", "127.0.0.1", "-p",
        str(runtime.PORTS["postgres"]), "-U", "temporal", "-d", "temporal",
        "-At", "-c", query], env=env, capture_output=True, text=True)


def director_http(token: str | None) -> int:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(f"http://127.0.0.1:{SERVICE_PORTS['director']}/",
        method="POST", headers=headers, data=b'{}')
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def disk_kib() -> int:
    return int(subprocess.check_output(["du", "-sk", str(STATE)], text=True).split()[0])


def footprint() -> dict:
    supervisor = int((STATE / "supervisor-ready").read_text())
    pg = int((STATE / "pgdata/postmaster.pid").read_text().splitlines()[0])
    return sample(supervisor, STATE, {}, {pg: "postgres_postmaster"})


def pending() -> int:
    with sqlite3.connect(STATE / "source/harness.sqlite3") as db:
        return db.execute("SELECT count(*) FROM pending").fetchone()[0]


async def held(client: Client, rows: list[dict]) -> list[dict] | bool:
    found = []
    for row in rows:
        task = await asyncio.to_thread(director_task, row["task_id"])
        parent = await client.get_workflow_handle(row["run_id"]).query(FactoryRun.status)
        if not parent["child_id"]:
            return False
        child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
        if (task["status"]["state"] != "input-required" or parent["phase"] != "awaiting-child"
                or child["phase"] != "parallel"):
            return False
        found.append({"run_id": row["run_id"], "task_id": row["task_id"],
            "child_id": parent["child_id"], "child_phase": child["phase"]})
    return found if pending() == len(rows) else False


async def main() -> dict:
    result = {"schema": "exomachina.temporal.package-ops/1", "prefix": str(PREFIX),
              "levels": {}, "security": {}, "schema_versions": {}, "status": "running"}
    try:
        no_auth = sql("select 1", password=None)
        yes_auth = sql("select 1", password=runtime.secret("temporal-password"))
        result["security"]["postgres"] = {"unauthenticated_exit": no_auth.returncode,
            "authenticated_exit": yes_auth.returncode, "authenticated_output": yes_auth.stdout.strip()}
        result["security"]["director"] = {"unauthenticated_http": director_http(None),
            "authenticated_http": director_http(runtime.secret("director-token"))}
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")
        await client.service_client.check_health()
        result["security"]["temporal_unauthenticated_health"] = "accepted"
        for database in ("temporal", "temporal_visibility"):
            r = subprocess.run([str(runtime.PG_BIN / "psql"), "-w", "-h", "127.0.0.1", "-p",
                str(runtime.PORTS["postgres"]), "-U", "temporal", "-d", database,
                "-At", "-c", "select curr_version from schema_version"],
                env=dict(os.environ, PGPASSWORD=runtime.secret("temporal-password")),
                capture_output=True, text=True)
            result["schema_versions"][database] = {"exit": r.returncode, "version": r.stdout.strip()}
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (STATE / "catalog/approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("visible-v3.json"), approved)
        digest = publish(package, STATE / "catalog", approved)
        result["package_digest"] = digest
        result["levels"]["0"] = {"pending": pending(), "state_disk_kib": disk_kib(), "footprint": footprint()}
        (STATE / "hold-source-assignments").write_text("ordinary bounded source wait\n")
        rows = []
        for target in (2, 10):
            for index in range(len(rows) + 1, target + 1):
                run_id = f"package-held-{index}-{PREFIX.name}"
                task = await asyncio.to_thread(director_send, {"op": "start",
                    "action_id": "start:" + run_id, "run_id": run_id, "package_digest": digest})
                rows.append({"run_id": run_id, "task_id": task["id"]})
            async def check():
                return await held(client, rows)
            current = await runtime.until(check, seconds=75)
            snapshot = footprint()
            after = await held(client, rows)
            if after is False:
                raise RuntimeError("held state changed during footprint sample")
            result["levels"][str(target)] = {"held": current, "after_sample": after,
                "pending": pending(), "state_disk_kib": disk_kib(), "footprint": snapshot}
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (PREFIX / "ops-observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


if __name__ == "__main__":
    observed = asyncio.run(main())
    print(json.dumps({"status": observed["status"], "levels": sorted(observed["levels"])}))
