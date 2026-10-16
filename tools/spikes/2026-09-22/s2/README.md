# S2 retained evidence

This directory preserves the bounded 2026-09-22 Strands harness spike and its real Kestra integration follow-on.

- **Latest outcome:** `kestra-result.md` and `kestra-result.json` — seven combined tests passed, including the real native-engine path.
- **Initial phase:** `result.md` — six fixture-engine harness/transport tests.
- **Implementation:** `harness.py`, `server.py`, `child_adapter.py`, `kestra_factory.py`, `kestra_adapter.py`.
- **Public-boundary tests:** `test_harness.py`, `test_a2a_process.py`, `test_kestra_integration.py`.
- **Native definition:** `kestra-flow.yaml`; the live test substitutes a unique S2 flow ID before publication.
- **Reproducible Python environment:** `pyproject.toml`, `uv.lock`, `.python-version`.

Run the six self-contained tests from this directory:

```sh
uv sync --frozen
uv run pytest -q --basetemp=.test-state
```

The seventh test is skipped by default. Running it requires the isolated Kestra 2.0.3 service at `127.0.0.1:28081` and its test Basic Auth file at `/tmp/exomachina-spikes/s1/auth.json`, as described in `kestra-result.md`:

```sh
EXOMACHINA_S2_KESTRA_LIVE=1 uv run pytest -q --basetemp=.kestra-final
```

**Sanitization:** no credentials file, database, state directory, process PID, log, runtime binary, virtual environment or live server state is included. Literal A2A bearer values in the tests are public fixture credentials, not production credentials. The live integration reads the parent's credential file without printing or copying its values. The machine summary retains synthetic artifact content, execution/identity IDs, public source hashes and native task histories as verification evidence.

`server.py` is retained owned entry-point source required by the tests. Background launcher/PID artifacts and the earlier live service are not part of this bundle. The source reports refer to fuller transient evidence under `/tmp/exomachina-spikes/s2`; the compact JSON here is the retained subset. Nothing in this bundle is a product deployment or general production qualification.
