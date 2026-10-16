# Hatchet embedded static-interpreter countertrial

22 September 2026 · macOS arm64 · bounded engine-choice evidence

**The no-worker-rollout behavior passed in this fixture, with a material lifecycle cost.** One unchanged Python worker registered one durable `factory` task and one generic work task. It ran v1, accepted a new v2 document while v1 waited, ran v2 with its extra `verify` step, then recovered both waits after its owner was killed. Two concurrent v1 decision events and one v2 event produced one synthetic delivery per run. A later status read found both Hatchet runs `COMPLETED`. The first crash-restart attempt failed because bundled PostgreSQL outlived the killed owner; an explicit, data-directory-scoped `pg_ctl stop` was needed before restart. The passing script includes that cleanup.

## Pin and topology

- Python 3.12.9; `hatchet-sdk==1.41.0`, with transitive versions in [uv.lock](uv.lock) (SHA-256 `ef04cfa70ce949a7d226bf37c6614ca4465bc4678bf25335b10e4adf61a51556`). The SDK package metadata says MIT.
- Hatchet embedded sidecar `v0.107.0`, Darwin arm64 binary SHA-256 `3903d6c7057ee2d1bc4d3809981287191e3106a1f3be812ae78f7d77855b98ff`. The [pinned engine license](https://github.com/hatchet-dev/hatchet/blob/v0.107.0/LICENSE) is MIT. The bundled PostgreSQL and transitive components still need ordinary distribution review.
- One owner Python process, one Go sidecar, Python worker subprocesses, and bundled PostgreSQL in a private persistent data directory. No Docker, separately provisioned database, paid service, model call, Strands harness, A2A endpoint, or web dashboard was used. The sidecar and PostgreSQL were downloaded on the first smoke run, then the sidecar was opened by exact path and checksum; this is **not yet an offline one-install package**.
- The owner ran with `start_api=False`. A separate final status check briefly restarted embedded mode with the local API enabled because the SDK `runs.get_status` uses that REST API. This was a read-only trial step; a product needs deliberate status, authorization, and API-exposure design.

The [official embedded-mode guide](https://docs.hatchet.run/v1/embedded) says Python starts a Go sidecar and bundles PostgreSQL, allows a supplied binary path/checksum, and documents separate data directories for concurrent instances. Its [local-running guide](https://docs.hatchet.run/v1/running-locally) positions embedded mode for local development and CI. This trial shows useful behavior, not a production guarantee for that topology.

## Observed sequence

The [publisher probe](probe.py) admitted only the two fixture shapes and required research, join, independent review, Director wait, and delivery in order. Its function would reject a missing or reordered gate, but this trial did not send a separate negative publication request. It wrote canonical content-addressed documents to a local catalog. The [static worker](service.py) passed the exact document and digest into the durable run input; it did not register a task for either factory version. V1 digest was `3a331c10…a57ea329`; v2 was `72c53db8…90f2d6`. The worker source hash was unchanged across v2 publication and process restart.

| Check | Observed |
| --- | --- |
| V1 and V2 publication | V1 and v2 reached `review` under the same owner process; only v2 recorded `verify`. Neither delivered before a decision. |
| Forced crash | The script sent `SIGKILL` to the owner process group while both runs waited. The first attempt to restart against the same data directory failed: PostgreSQL had daemonized and retained its `postmaster.pid`. |
| Recovery with local cleanup | The revised script stopped only that trial's PostgreSQL data directory using its bundled `pg_ctl`, then restarted the same engine/worker code. Both run IDs and definitions persisted. |
| Duplicate Director events | Two threads pushed distinct approval events for the same v1 run; one event approved v2. The synthetic SQLite receipt table recorded one `delivery` per run. This does not prove Hatchet suppresses every duplicated external effect; the receipt has a unique key. |
| Terminal state | [Status check](status-check.json) found both stored Hatchet workflow runs `COMPLETED` after another clean engine restart. |

The final [observed data](observed.json) contains all run IDs, document digests, step records, event-push IDs, owner PIDs, and paused process samples. The final recovery log showed the engine ready at 19:14:10 local time and both factory tasks finishing at 19:15:12. The roughly one-minute gap is an observation for this untuned crash sequence, not a measured steady-state latency or a diagnosed lease value. The synthetic work was fast; no real remote A2A operation was involved.

## Resource and implementation cost

With two runs paused, `ps` saw three Python processes totaling **219,360 KiB RSS**, one sidecar at **81,408 KiB**, and 30 PostgreSQL processes totaling **622,000 KiB**. The raw sum is **922,768 KiB (about 901 MiB)**. PostgreSQL processes share pages, so this sum **double-counts memory** and must not be interpreted as physical footprint or compared directly with one-process RSS from another candidate. The Python total includes the worker's subprocesses but excludes a real Strands harness, A2A server, artifacts, and model clients. The Python virtual environment used 93 MiB on disk; the cached sidecar 54 MiB; this trial's initialized PostgreSQL directory 221 MiB; the shared PostgreSQL binary cache 32 MiB. These were not clean-package measurements.

The fixed interpreter and local owner are 161 source lines in `service.py`; the publisher, process-control and assertions are 176 lines in `probe.py`; the smoke and status scripts add 34 and 33 lines. These are narrow fixture sizes, not production estimates. The accepted document shape is deliberately small, so this does not establish arbitrary approved-block graph coverage. Immutable publication, policy validation, capability contracts, actor authorization, artifact acceptance, effect reconciliation, and interpreter compatibility would be Exomachina-owned work. The output ledger is a synthetic SQLite table outside Hatchet's PostgreSQL transaction. In a real remote-effect crash window, stable effect IDs and provider-confirmed idempotency or reconciliation remain necessary.

The official multiple-instance guidance allows distinct bundled data directories or a fleet sharing an external PostgreSQL database. We tested **one** embedded owner with two logical harness IDs, not two independent Strands harness processes. A per-harness embedded sidecar/PostgreSQL would repeat much of this process tree; a shared installation would need one lifecycle owner, migration ordering, port/identity policy, backup, and protected access. The observed orphaned postmaster after `SIGKILL` makes that ownership concrete. Automatic keysets stored beside the database also need replacement for meaningful at-rest separation, as the embedded-mode guide notes. Production support and security for this topology remain unproved.

## Reproduce

From this directory on macOS arm64 with Python 3.12/uv:

```sh
uv sync --python 3.12 --frozen
HATCHET_SPIKE_DATA_DIR=/tmp/exomachina-hatchet-r2-smoke uv run --frozen python smoke.py
uv run --frozen python probe.py
uv run --frozen python inspect_status.py
```

The smoke downloads a pinned sidecar and bundled PostgreSQL on first use. The probe uses a fresh temporary data directory, kills only its own process group, explicitly stops its own orphaned PostgreSQL before restart, writes `observed.json`, and stops the final instance. The status script starts a temporary local API to inspect those run IDs, writes `status-check.json`, and stops its instance. A product package must supply a platform-pinned binary rather than rely on first-use downloads. No owned process remained after the final check.
