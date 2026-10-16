# S4 remote-work and acceptance evidence

[result.md](result.md) distinguishes three bounded cases:

1. `spike.py` uses a **custom HTTP receiver and stub engine** to force a lost remote acknowledgement, prove cooperative discovery/deduplication, retain an opaque `unknown` outcome, and replay a product outbox after exits. Observation: [custom-http-result.json](custom-http-result.json).
2. `kestra_bridge.py` replaces the stub continuation with a **real Kestra 2.0.3** `Log → Pause → Log` run, while the receiver remains custom HTTP. Observation: [kestra-bridge-result.json](kestra-bridge-result.json).
3. `a2a-gated.yaml` and `gated.py` use a **native Kestra HTTP task → real Strands A2A 0.3.0 endpoint → product exact-artifact acceptance → Kestra Pause/resume**. The A2A task completed immediately; the forced exits covered product observation and engine continuation, not a lost Kestra-to-A2A reply. Observation: [kestra-a2a-gated-result.json](kestra-a2a-gated-result.json) and [observed-a2a.json](observed-a2a.json). `a2a.yaml`, [a2a-result.json](a2a-result.json) and [a2a-task-output.json](a2a-task-output.json) retain the earlier transport-only case.

The scripts are snapshots from `/tmp/exomachina-spikes/s4` and expect that scratch layout, local Kestra credentials and an isolated server. They do not install or securely configure the runtime. Do not interpret the fixture reviewer identity, A2A bearer token, local HTTP listener or HTTP 200 as production authorization or semantic acceptance. No ephemeral database, password, runtime binary, virtual environment or log is included.
