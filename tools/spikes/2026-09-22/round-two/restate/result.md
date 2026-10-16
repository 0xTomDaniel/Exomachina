# Restate stable-handler factory-document countertrial

22 September 2026 · **Bounded runtime trial passed; product engine unselected**

## Topology and artifacts

One self-hosted Restate server **1.7.10** ran as a macOS arm64 binary with its own persistent local directory. One unchanged Python **restate-sdk 1.0.5** handler served a fixed `FactoryRun` workflow under Hypercorn. There was no Docker, PostgreSQL, model call, Strands harness, A2A peer, or product authorization gateway. The server binary was installed via `@restatedev/restate-server@1.7.10`; its SHA-256 in this trial was `4f655ad702caa80070d863991faf01f3c262b8b9726ed0ecc7f80fae534eb030`. The server archive/package tree occupied 204,900 KiB and this minimal Python virtual environment 8,352 KiB. The [observed run record](observed.json), [fixed handler](server.py), [driver](probe.py), and [v1/v2 documents](fixtures/v1.json) are retained here; runtime state and logs were under a temporary directory.

Restate's server is [BUSL-1.1, not OSI open source](https://github.com/restatedev/restate/blob/v1.7.10/LICENSE). Its inspected additional grant appears to permit an independently branded public workflow product behind a GUI/DSL/proprietary Interface while direct third-party Restate endpoint registration/invocation remains restricted. This is a **conditional rights screen, not legal clearance**. The Python SDK is [MIT licensed](https://github.com/restatedev/sdk-python/blob/main/LICENSE). The proposed Exomachina product must retain its own factory Interface and verify the assembled distribution/API arrangement before adoption.

## Observed behavior

The trial published content-addressed v1, started `harness-1-v1`, and reached a durable Director wait. It then published a structurally different v2 with an added `verify` step **without restarting the Python handler** and started `harness-2-v2`. Both runs reached the wait. The driver sent SIGKILL to the handler and Restate server, restarted both against the same persistent directory **without re-registering the handler**, and sent three concurrent decisions: two for v1 and one for v2. One v1 decision returned 200, the duplicate returned 409 (`promise was already completed`), and the v2 decision returned 200. Both workflows attached successfully. V1 returned its original digest and version 1 without `verify`; v2 returned its own digest and version 2 with `verify`. The append-only synthetic event log recorded each run's `deliver` once and every completed node once.

The two research nodes used the SDK's documented [`restate.gather` durable-future combinator](https://docs.restate.dev/guides/parallelizing-work) before the join. An initial trial using ordinary `asyncio.gather` progressed through the two research nodes on v2 but did not advance to the join within the 30-second bound; replacing it with `restate.gather` passed. This is an authoring-pattern observation, **not** evidence of an engine concurrency defect. Another initial run used the wrong `run_typed` positional argument shape and was corrected before the passing result. Mac's Unix-socket path length also required `--listen-mode tcp` for the long temporary base directory.

| Measurement | Observed | Scope |
| --- | ---: | --- |
| Warm server RSS before flows | 114,912 KiB | Restate server only |
| Warm handler RSS before flows | 35,264 KiB | Python handler only |
| Paused server RSS after two runs | 249,248 KiB | Restate server only |
| Paused handler RSS after two runs | 35,264 KiB | Python handler only |
| Initialized server state | 692 KiB | Synthetic two-run state after completion |

The two paused RSS values sum naively to about 278 MiB, but shared pages can make RSS sums differ from whole-machine physical memory. These figures exclude Strands, A2A, artifacts, UI, active model calls and load. The driver and server logs had stopped before this result was written. The observed binary is materially larger on disk than the minimal Python environment.

## Product work and limits

The handler interprets only this narrow profile: two parallel research actions, linear join/review or join/verify/review, Director wait, and delivery. The trial-owned publisher verifies a digest and a few allowed-node/review-order checks. It is **not** a complete flexible graph language, semantic validator, immutable dependency closure, publication service, compatibility policy or diagnostics UI. The Python handler is 70 source lines; the 193-line driver is test machinery, not production code. These counts do not estimate full product effort. Restate owns durable workflow identity, journaled steps, wait persistence and duplicate promise resolution in this fixture. Exomachina would own the factory catalog/interpreter, actor authorization, accepted-artifact ledger, remote A2A reconciliation and permitted-effect controls.

The test used run IDs labelled as two harness identities, but did not launch two Strands processes or protect one actor's workflow from another. It did not test a lost remote A2A acknowledgement, independent child factories, bounded repair loops, versioned capability contracts, user permissions, server upgrades, backup, multi-node operation or a clean-machine package. The local synthetic file append is not an external exactly-once guarantee. The Restate candidate clears **the no-rollout v1/v2 publication and local restart gate** at this narrow scope, with the definition layer and distribution rights still material selection costs.

## Reproduce

On macOS arm64 with Node/npm and Python 3.14/uv available:

```sh
npm install --prefix /tmp/exomachina-round2-restate-server --no-audit --no-fund @restatedev/restate-server@1.7.10
cd tools/spikes/2026-09-22/round-two/restate
uv sync --python 3.14 --frozen
RESTATE_SERVER_BIN=/tmp/exomachina-round2-restate-server/node_modules/@restatedev/restate-server-darwin-arm64/bin/restate-server uv run --frozen python probe.py
```

The driver uses local ports 8080, 9070 and 19080 and creates isolated temporary state. It stops its child processes in `finally` and writes `observed.json`. Port conflicts must be cleared before running. The result and license review should be redone on the pinned target platform before shipping.
