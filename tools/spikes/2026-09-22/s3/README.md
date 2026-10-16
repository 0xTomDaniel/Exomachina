# S3 native publication evidence — 22 September 2026

This bundle preserves the bounded Kestra experiment described in [result.md](result.md).
It contains eight native YAML fixtures, the API probe and fixture generator,
selected unchanged API responses, and the original runtime summary. Original-file
hashes are recorded in [bundle-provenance.json](evidence/bundle-provenance.json).
The scripts were adapted after the run to require external credentials/output;
that portability change was syntax/CLI checked, not a repeat runtime experiment.

## Reproduction prerequisites

- Kestra OSS **v2.0.3**, source commit
  `269e8d0d01c27f6117a667758112d2b8d77cc4a3`; JAR SHA-256
  `b4b4518a617f93965dcf98c9673beda3dad2b6d9d1780ad8b9a945e437975ab4`.
- The observed setup used Temurin **25.0.4.1+1**, PostgreSQL **16.15**, local
  storage, four worker threads and Python **3.14.3**. The scripts use only the
  Python standard library (Python 3.10+ syntax).
- Start an isolated, PostgreSQL-backed `server standalone` outside the checkout,
  with unique HTTP/management/controller/database ports. The observed HTTP API
  was `http://127.0.0.1:28084/api/v1/main`. S0 owns runtime package/startup evidence;
  this bundle does not install Java, PostgreSQL or Kestra.
- Use a fresh database or unused `exomachina.s3` namespace. The probe creates and
  updates flows and expects native revisions 1 and 2. Do not target a production
  or shared experiment namespace.
- Supply matching temporary Kestra Basic Auth credentials in an **external**,
  mode-0600 JSON file with `username` and `password` fields. No credential file or
  runtime configuration is included here.

From this directory:

```sh
python3 run_probe.py \
  --base-url http://127.0.0.1:28084/api/v1/main \
  --auth-file /tmp/kestra-s3-auth.json \
  --evidence-dir /tmp/kestra-s3-replay-evidence
```

The probe reads the checked-in YAML and writes new responses only to the external
directory. `--finish` re-reads execution IDs already recorded there and retrieves
their outputs without publishing or starting new executions. Optional fixture
regeneration uses `python3 prepare.py --output-dir /tmp/kestra-s3-fixtures`.
Runtime shutdown remains the operator's responsibility.

## Limits

The narrow revision-binding mechanics passed. Complete S3 did not: deterministic
Return tasks stand in for agents, independent Quality and delivery. No real
review authorization, acceptance records, external effects, plugin updates,
capability immutability, publisher concurrency, A2A or production capacity was
proved. The delivery-only bypass was accepted by native validation/publication
but deliberately not executed. No database, logs, binaries, upstream source,
configuration, credentials or raw HTTP headers are preserved in this bundle.
