# Kestra local bundle trial

This is the sanitized source and result of a **macOS arm64 host-local feasibility test**, not an installer or a proven clean-machine package. `result.md` records the measured one-command start and durable restart with bundled PostgreSQL, Kestra, and JDK processes, plus the unresolved distribution and portability gates.

Retained files:

- `result.md` — commands, observed behavior, dependency closure, and limits.
- `start.sh` — trial one-command launcher; it generates its own local credentials at runtime.
- `bundle_deps.py` — trial build-time dylib copy/patch/ad hoc signing script; it requires the pinned Homebrew formulae on the build host.
- `flow.yml` — credential-free `Log → Pause → Log` restart fixture.

The executable JAR, JDK, PostgreSQL binaries/dylibs, database and artifact state, credentials, generated configuration, logs, and API responses are deliberately absent. The retained scripts alone cannot run until those pinned artifacts are supplied. The original scratch trial is under `/tmp/exomachina-spikes/s0/kestra/package` on the test host.
