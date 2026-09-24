# Qualification spikes A–C: common rules (read first)

Integration branch `qual/factory-spikes` (from `origin/main` 374b27a), worktree `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-qual`. Each spike works in its own worktree and branch, listed in its brief. The orchestrator merges, reviews and commits. **Do not commit, push, merge, rebase or switch branches.** Leave your changes uncommitted in your worktree.

`INTERFACES.md` still binds, including **no deletion** (no `rm`, `unlink`, `rmtree`, `git clean`, `git checkout -- <file>`, and so on). If a command or approval is rejected, stop that action and report it; do not route around it.

## Architecture that must hold

- The Strands harness in factory mode sits behind its normal A2A identity and `verified-research@1` card. There is no separate factory endpoint, and callers never choose a graph, package or version.
- One shared, lazy Temporal/PostgreSQL runner per install home serves every harness instance there. Harness startup without unfinished runs never starts it, and no harness stops it.
- External agent nodes are black boxes reached only through A2A, their Agent Card, and a declared application-level contract. Product code must not import their code or read their state.
- Lean scope: no general directory, no separate factory endpoint, no multi-tenancy, sandbox, memory ceiling, installer or engine comparison. The HTTP release fixture stays.

## Honesty rules

- Every claim in evidence is labelled `observed-real` (real processes: harness, A2A, Temporal, runner, live broker where stated), `observed-synthetic` (real code against a fixture or mock), or `unit-tested`.
- A passing fixture or mock is never described as live proof. Label the Director model `fixture` or `live` in every evidence file.
- Record falsification honestly. If a pre-registered check fails, capture the failure first, then fix it, and record both the failure and the fix.

## Mechanics

- Python: `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python -B` (absolute; the venv is not in your worktree).
- In your worktree, run `npm --prefix prototype/temporal-factory/broker ci --offline --cache /tmp/exomachina-pi-strands-debate/npm-cache` once before tests that touch `broker/`.
- Trial state: only under `/tmp/exo-qual-<spike>-<suffix>/`. Stay in your port block:

| Spike | Runner `port_base` / `member_base` | Harness ports | Testbed / services | Unit mocks |
| --- | --- | --- | --- | --- |
| A | 44200 / 32440 | 44840–44841 | 45400–45419 | 46200–46249 |
| B | 44300 / 32460 | 44850–44854 | 45500–45519 | 46250–46299 |
| C | 44400 / 32480 | 44860–44861 | 45600–45619 | 46300–46349 |

- Keep the existing unit suite green: `cd prototype/temporal-factory && $PY -B -m unittest discover -s tests` (90 tests on main). Report the before and after counts.
- Evidence goes to `prototype/temporal-factory/evidence/spike-<x>/`. Handoff goes to `prototype/temporal-factory/handoff/spike-<x>[-part].md`. Include:
  - files changed;
  - exact commands with pass/fail output;
  - each pre-registered check with its verdict and evidence path;
  - trial directories, left in place;
  - limitations;
  - anything you could not do.
- Before your final report, stop every process you started: services, harnesses, runner (`src/runner.py stop --home H`) and broker only if you started it. Confirm no listener remains in your port block.
- When finished, reply with a short summary: verdict per check, test counts, and the handoff path.
