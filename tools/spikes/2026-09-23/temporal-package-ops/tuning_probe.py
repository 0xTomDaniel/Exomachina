"""Measure a running bundle and PostgreSQL schema/config without mutating it."""
import json
import os
import subprocess
from pathlib import Path

import runtime
from footprint_probe import sample

prefix = Path(__file__).resolve().parent.parent
state = prefix / "state"
runtime.STATE = state
supervisor = int((state / "supervisor-ready").read_text())
pg = int((state / "pgdata/postmaster.pid").read_text().splitlines()[0])
password = (state / "secrets/temporal-password").read_text().strip()
def sql(command):
    return subprocess.check_output([str(runtime.PG_BIN / "psql"), "-w", "-h", "127.0.0.1", "-p",
        str(runtime.PORTS["postgres"]), "-U", "temporal", "-d", "temporal", "-At", "-c", command],
        env=dict(os.environ, PGPASSWORD=password), text=True).strip()
result = {"schema": "exomachina.temporal.package-tuning/1",
    "shared_buffers": sql("show shared_buffers"), "max_connections": sql("show max_connections"),
    "membership_port_type": sql("select data_type from information_schema.columns where table_name='cluster_membership' and column_name='rpc_port'"),
    "postgres_connections": int(sql("select count(*) from pg_stat_activity")),
    "footprint": sample(supervisor, state, {}, {pg: "postgres_postmaster"}),
    "state_disk_kib": int(subprocess.check_output(["du", "-sk", str(state)], text=True).split()[0])}
label = os.environ.get("EXO_TUNING_LABEL", "sample")
(prefix / f"tuning-{label}-observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
print(json.dumps({k: v for k, v in result.items() if k not in {"footprint"}}, sort_keys=True))
