# S1 contested-state evidence

[result.md](result.md) describes a single Kestra OSS 2.0.3 standalone server with PostgreSQL 16.15. [result.json](result.json) records the stopped/restarted paused execution and eight concurrent resume responses; [race-results.json](race-results.json) records five resume/kill races. [pause.yaml](pause.yaml) is the native no-side-effect flow. `probe.py` and `race.py` are bounded test drivers.

The scripts expect a fresh isolated test server and an external `auth.json` containing temporary Kestra Basic Auth credentials in the script directory. That file and all server state were omitted. The current v2 route and multipart body are used. This is a behavioral record for one local server, not proof of multi-server locking, external effect idempotency, production authorization or throughput.
