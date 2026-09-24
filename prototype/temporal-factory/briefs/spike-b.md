# Spike B: two factory-mode harness instances in one install home

Read `briefs/qual-common.md` first. Worker: **tw_package**. Worktree: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-qual-b` (branch `qual/spike-b`).

## Question

Can two factory-mode harness processes share one install home and its single lazy runner while keeping identities, catalogs, Tasks, results and effects separate? Each process has its own durable identity, catalog, A2A port and Agent Card.

## Files you own

- New `scenarios/spike_b_two_instances.py`, new `tests/test_same_home.py`, `evidence/spike-b/*`, `handoff/spike-b.md`.
- Fixes only where a pre-registered check fails:
  - `src/runner.py`;
  - `src/admin.py` (`provision` only);
  - in `src/harness.py`, **only** `init_instance`, `load_config`, `create_app`'s startup, recovery and `/health` code, and the `__main__` block.
- Spike C concurrently owns `Director.invoke`, `Director.perform` and `HarnessExecutor`/`harness_server.py`. Do not touch them. Keep your `harness.py` diff small and localized.

## Setup

- One home `H=/tmp/exo-qual-b-<suffix>`, with the runner at `port_base=44300` and `member_base=32460`.
- The testbed is at 45500.
- Instances `alpha` (port 44850) and `beta` (port 44851) live in `$H/instances/{alpha,beta}`. Provision and publish v1 in each separately, with `admin.py`. Each gets its own catalog and publication, so digests may coincide. Identities must differ.
- Use the fixture Director and the existing testbed. This spike is about process and runner topology, not models.

## Pre-registered falsifiable checks

| ID | Check | Pass | Fail |
| --- | --- | --- | --- |
| B-1 | Cold concurrent first requests | Runner not running. Both harnesses are started, `/health` shows `runner_running:false`, and no `pgdata` exists. Fire `message/send` `start` to alpha and beta concurrently, released by a barrier. `runner-events.jsonl` shows exactly one `serve-ready` and exactly one `start` event, plus an `attach` for the other, and one `serve` PID. Both Tasks reach `completed` with their own artifact, `run_id` prefix = own identity, and own release receipt | Two `serve` processes, two `serve-ready` events, a crash, or a failed or cross-bound Task |
| B-2 | No cross-instance contamination | A Task id from alpha is unknown to beta (`tasks/get` → not found), and vice versa. The outcome journal action ids and release receipts partition cleanly by instance identity. Each instance's `director.sqlite3` holds only its own runs. Each Agent Card shows its own name and url | Any cross-visible Task, result or effect |
| B-3 | One instance down, the other continues | Start a run on alpha that parks at a Director wait (`outcome_mode:"never"`, so it becomes `input-required` after repair exhaustion) and a run on beta. SIGTERM alpha while beta's run is in flight. Beta's run completes. The runner PID is unchanged, and there is no `stop` event or runner restart | The runner stops or restarts, or beta's run fails |
| B-4 | Restarted instance reattaches | Restart alpha: same identity, incarnation +1. `/health` shows `recover-unfinished` attached, **not** a new runner start. `tasks/get` on the original alpha Task shows `input-required`. An `abort` on that original Task completes it as `aborted`, with 0 releases for that run | A new runner, a lost Task, or a different run |
| B-5 | Hard-kill variant | Repeat B-3/B-4 with SIGKILL of alpha during an active (not parked) run. Its Temporal run completes while alpha is down, and alpha projects the result on its original Task after restart | Lost or duplicated result |
| B-6 | Same-home port and config handling | Each must be rejected with a clear error and without disturbing the running peer: (a) provisioning a third instance with alpha's harness port; (b) provisioning with a runner `port_base`/`member_base` different from the home's runner; (c) reusing an existing instance name with different config; (d) starting a second process for an already-serving instance directory, which must not fence or steal the live one. Also show that the runner config belongs to the home, not the instance | Silent acceptance, a second runner on other ports, or the live instance fenced |

Report the architectural implications:
- whether the runner's config ownership should move from `instance.json` to the home;
- what a process-level instance lock needs;
- what remains unproven: more than two instances, and stress.

**A likely finding to check, not to assume:** today each instance's `instance.json` carries its own runner ports, and `Director.__init__` bumps the incarnation on every open. Test (b) and (d) against the unmodified code first, and record the result before any fix.
