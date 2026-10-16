# Effect copied-bundle and local lifecycle trial

22 September 2026. **Passed on one macOS arm64 host.** A copied, host-local bundle started two independent Strands factory Directors, one Strands capability, one Strands Quality service, and one shared Effect helper with a single `run` command. A small bundled supervisor restarted one killed Director without changing the other Director's PID or waiting A2A task. It also restarted the shared Effect helper. A separate test killed and relaunched the supervisor itself; both original A2A task IDs survived on the same ports and completed. This is a concrete path to the permitted bundled-local-helper topology. It is **not** evidence of a portable, clean-machine, network-isolated, or redistributable installer.

## Build and reproduce

The source scripts are [make_bundle.py](make_bundle.py), [bundle_launcher.py](bundle_launcher.py), and [bundle_probe.py](bundle_probe.py). Build inputs were the official Node.js 26.0.0 macOS arm64 tarball, SHA-256 `880cf6f35eb9dea84b2373adba13b6023b50cc0decbad47b57824d146373265a`, a uv-managed CPython 3.13.1 runtime, Effect's pinned [npm lock](package-lock.json), and the pinned [S2 uv lock](../../s2/uv.lock). The build used host `uv` 0.6.9 to install the locked Python dependencies into the copied bundle. The Node tarball digest was checked against the official [Node 26.0.0 distribution manifest](https://nodejs.org/dist/v26.0.0/SHASUMS256.txt) before extracting only the Node executable. The bundle uses a different Python minor version than the earlier 3.14.3 core trial; this follow-on actually executed the same Strands/A2A path on 3.13.1.

```sh
curl -fLsS https://nodejs.org/dist/v26.0.0/node-v26.0.0-darwin-arm64.tar.xz \
  -o /tmp/exomachina-node-v26.0.0-darwin-arm64.tar.xz
uv python install 3.13.1 --install-dir /tmp/exomachina-uv-python
python3 make_bundle.py \
  --output /tmp/exomachina-effect-bundle-2 \
  --node-archive /tmp/exomachina-node-v26.0.0-darwin-arm64.tar.xz \
  --python-runtime /tmp/exomachina-uv-python/cpython-3.13.1-macos-aarch64-none
python3 bundle_probe.py \
  --bundle /tmp/exomachina-effect-bundle-2 \
  --state /tmp/exomachina-effect-bundle-state-fresh
python3 supervisor_restart_probe.py \
  --bundle /tmp/exomachina-effect-bundle-2 \
  --state /tmp/exomachina-effect-bundle-supervisor-state-fresh
```

The single runtime start command is `/tmp/exomachina-effect-bundle-2/run --state /tmp/exomachina-effect-bundle-state-fresh`. It uses only copied source, Node executable and dependencies, Python interpreter and installed environment. The test driver invokes this command, exercises the A2A endpoints, then terminates it; the launcher stops its children. Runtime state, logs, PIDs and ports stay under `/tmp`, not in the repository. Use a fresh output and state path for each reproduction.

## Observed lifecycle

The [bundle evidence](bundle-observed.json) is from fresh state `/tmp/exomachina-effect-bundle-state-6`; it embeds the bundle manifest with pinned build inputs and source hashes. The supervisor launched five child processes and exposed five loopback HTTP ports. The two Directors had different persistent identities and each started one factory through real A2A `message/send`. Both runs reached `input-required` at the Effect Director wait.

The driver sent `SIGKILL` to Director A. The supervisor restarted only A against its state, preserving its identity and incrementing its incarnation. Director B retained its PID and original A2A task at `input-required`; A's original task was also retrievable. The driver then killed the shared Effect helper. The supervisor restarted only the helper against its SQL/product state. Both Directors retained their PIDs and both tasks remained waiting. Director B and then Director A sent decisions; the original A2A tasks completed with exact accepted artifacts. Both product ledgers had one delivery; each capability and Quality action had `accepted_count=1`.

| Process | Warm RSS KiB | Two runs waiting RSS KiB |
| --- | ---: | ---: |
| Supervisor | 33,344 | 33,328 |
| Shared Effect helper | 203,376 | 221,184 |
| Capability Strands harness | 106,400 | 107,632 |
| Quality Strands harness | 107,264 | 109,232 |
| Director A Strands harness | 105,408 | 109,264 |
| Director B Strands harness | 105,376 | 109,520 |
| **Sum of process RSS** | **661,168 KiB (645.7 MiB)** | **690,160 KiB (674.0 MiB)** |

RSS sums can double-count shared pages. This is one host and workload, not a controlled comparison against other engines. The first cold manual launch reached ready in about **10.0 s**; the recorded probe launch after caches were warm took **4.25 s** wall time. The installed copied bundle used **401,972 KiB (392.6 MiB)**, including roughly 148,480 KiB for the official Node executable, 85,052 KiB for Node dependencies, 52,488 KiB for the Python runtime, and 105,372 KiB for the locked Python environment. The probe's separate state and logs used **1,360 KiB** at measurement. The Python environment includes S2 test dependencies and is not size-optimized.

The helper is a **shared local child of the bundle supervisor**, not one Node/Effect process per Director. In this sample, each additional Director Python process used roughly 105–109 MiB RSS; the shared helper used roughly 200–221 MiB. This shape keeps helper cost from multiplying per factory Director within one installation. It also makes the supervisor, local port routing, process restart, and shared helper state part of Exomachina's owned product surface.

## Independence and remaining limits

The recorded process commands and copied Python `sys.path` contain only bundle/state paths, not the Exomachina source checkout. The copied venv's Python symlink and `base_prefix` resolve inside the bundle. Inspection of the bundled Node executable, Python executable, native Node addon, and 22 Python native modules found only macOS system-library dependencies or self-references; the official Node binary avoided the Homebrew-specific dynamic dependencies of the locally installed `node`. No `npm`, `uv`, Homebrew service, Docker, JVM, or source checkout was invoked at runtime.

This was **not** a clean-machine or strict offline test. The build downloaded Node, CPython and some locked Python wheels, and the launch ran on the same development Mac with network available. The venv's symlink and `pyvenv.cfg` contain absolute paths **within the output bundle**, so moving the built directory to a different path was not proven and may break it. Mac system frameworks remain required; Linux, Windows, older macOS, and another Apple Silicon machine were not tested. Complete transitive redistribution notices and shipped-artifact rights were not audited. The local supervisor has no production restart backoff, upgrades, migration, health-based replacement, auth/TLS, tenant isolation, or cross-host recovery. The Director still forwards a Quality identity string for acceptance in this fixture; it does not prove authenticated Quality authority.

## Supervisor process-loss recovery

The first launcher iteration chose fresh ports at every startup and would have stranded the original A2A URLs after a supervisor crash. The [current launcher](bundle_launcher.py) persists a port topology before launch and holds a single-supervisor file lock. On relaunch it checks each prior child's PID, exact command, and process start time before terminating those orphaned local children. It then restarts every role on the same ports and state. It fails closed if a recorded PID has become an unrelated process. This is **216 lines of product-owned Python**, including ordinary child restarts and supervisor recovery; Effect does not supply it.

The fresh [supervisor fault evidence](supervisor-observed.json) killed the supervisor with `SIGKILL` while two original Director A2A tasks waited. All five children remained alive as orphans. A second `run --state` claimed the persisted topology, stopped those exact children, and launched replacements on the **same five ports**. All four Strands identities were preserved with incremented incarnations. The original task IDs `a4a7340a-a256-42fe-afa0-3d1e42781900` and `a9e76d3b-6f47-45e5-bd3a-08f60e61345a` were again `input-required`, then both completed. Receiver assignment and Quality actions each had one accepted effect, and each factory recorded one product delivery. The first supervisor exited `-9`; the replacement exited cleanly with code 0.

This passes the same-host supervisor-restart gate for **settled waiting tasks**. A crash after launching a child but before recording its PID could leave an unrecorded orphan. Host reboot, filesystem loss, port theft, and cross-host takeover remain untested. The process inspection uses macOS `ps` output and is a trial implementation, not a portable production lifecycle policy.
