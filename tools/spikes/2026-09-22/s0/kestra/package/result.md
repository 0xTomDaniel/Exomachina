# S0 follow-on — local Kestra bundle feasibility on macOS arm64

**Decision:** The pinned Kestra OSS stack passed a bounded **host-local one-command runtime trial** without using Homebrew binaries or dylib load paths at runtime. This is evidence that a Docker-free bundled helper process is feasible on this machine. It does **not** prove a redistributable one-install product, clean-machine portability, an installer, update/backup behavior, or full license/NOTICE clearance. Stop this spike here; those are product packaging work, not further workflow-engine selection probes.

## Pinned materials and provenance

- [Kestra OSS v2.0.3 release](https://github.com/kestra-io/kestra/releases/tag/v2.0.3): copied `kestra-2.0.3` executable JAR, SHA-256 `b4b4518a617f93965dcf98c9673beda3dad2b6d9d1780ad8b9a945e437975ab4`, matching the [release checksums](https://github.com/kestra-io/kestra/releases/download/v2.0.3/checksums_sha256.txt). Its pinned [LICENSE](https://github.com/kestra-io/kestra/blob/v2.0.3/LICENSE) is Apache 2.0; transitive redistribution remains open in the parent [S0 audit](../result.md).
- [Eclipse Temurin](https://adoptium.net/temurin/releases/) JDK 25.0.4.1+1-LTS copied under `runtime/Contents/Home`; `java --version` reported that exact version. The trial did not use system Java or a separately installed JDK.
- PostgreSQL 16.15, compiled for Apple arm64/Darwin 25.6.0, copied from a Homebrew bottle keg under `pg/` and run with a private database cluster. The original keg's `INSTALL_RECEIPT.json` identifies a poured bottle and exact runtime formula versions. The [PostgreSQL license](https://www.postgresql.org/about/licence/) permits broad use, but the redistributed bundle would also include third-party dylibs and needs its own license/NOTICE review. The [Kestra runtime/storage guide](https://kestra.io/docs/configuration/runtime-and-storage) describes PostgreSQL as a normal durable queue/repository path; this trial did not use H2.
- Trial host: macOS 26.5.2, arm64. Bundle size after the test: **609 MiB**: copied Kestra/JDK `runtime/` **418 MiB**, patched PostgreSQL keg and libraries `pg/` **125 MiB**, generated state `data/` **66 MiB**. These are `du -sh` values, not a compressed installer size or a steady-state capacity benchmark.

## Bundle construction and dependency closure

The spike artifacts remain in this scratch directory. `bundle_deps.py` copies required libraries from the **build host's** `/opt/homebrew` formulae into `pg/lib/bundle`, rewrites absolute Mach-O dependency loads to relative `@loader_path` paths, copies ICU's separately installed `libicudata.78.dylib`, and ad hoc signs patched Mach-O files. The copied sources include PostgreSQL client libraries and gettext, OpenSSL, Kerberos, ICU, LZ4, Zstd, and Readline libraries. Build-time Homebrew dependency is explicit; the test launcher does not call it.

Representative observed output:

```text
$ python3 bundle_deps.py
processed=163 patched=0 copied_dylibs=18 signed=143
$ pg/bin/initdb --version
initdb (PostgreSQL) 16.15 (Homebrew)
$ pg/bin/postgres --version
postgres (PostgreSQL) 16.15 (Homebrew)
$ pg/bin/pg_ctl --version
pg_ctl (PostgreSQL) 16.15 (Homebrew)
```

`patched=0` is an idempotent rerun after the initial patch; the initial run rewrote the absolute loads. A scan of all **143** Mach-O executable/dylib files under copied `pg/bin` and `pg/lib` found **zero** `/opt/homebrew` dependency loads and **zero** `/opt/homebrew` rpaths. **22** dylib self-identification strings still contain their original Homebrew paths; they were not load references. While PostgreSQL ran, `lsof -p <postmaster PID>` showed the executable and external dylibs loaded from this bundle and **zero** open files under `/opt/homebrew`. The Temurin `java` executable's `otool -L` showed `@rpath/libjli.dylib` and a macOS system library, with no Homebrew path. This is strong evidence for this specific host run; it is not a clean-machine test or an audit of every possible optional PostgreSQL feature.

The first `env -i` launch failed because PostgreSQL needs a valid locale when starting; the launcher now explicitly sets `LC_ALL=C.UTF-8`. The first generated HTTP password lacked Kestra's uppercase requirement; the launcher now generates a value with upper, lower, and digit characters. These were launcher fixture fixes. The final tested script is `start.sh`.

## One-command cold start, stop and durable restart

The final launcher uses only its copied PostgreSQL/JDK/JAR paths plus standard macOS commands. It initializes its own PostgreSQL cluster on the first run, uses SCRAM authentication on loopback, creates its database, generates a private Kestra configuration and Basic Auth credential (mode 0600), starts Kestra on loopback, and shuts both processes down when the launcher receives `SIGTERM`. Its persistent queue and repository use PostgreSQL; its artifact storage is local disk. It was invoked with a restricted path to exclude Homebrew executables:

```sh
cd /tmp/exomachina-spikes/s0/kestra/package
env -i HOME="$HOME" PATH=/usr/bin:/bin:/usr/sbin:/sbin ./start.sh
```

Observed sequence against `http://127.0.0.1:28087` using a local credential file (never included in this report):

1. `POST /api/v1/main/flows` published `flow.yml`, a `Log → Pause → Log` flow; HTTP 200, revision 1.
2. `POST /api/v1/main/executions/exomachina.spike/pause_restart` created execution `5F7VtHUdjnNoJk7xeHofZH`; it reached `PAUSED` with `before=SUCCESS`, `approval=PAUSED`.
3. Sent `SIGTERM` to the launcher and observed both Kestra and PostgreSQL ports close and the PostgreSQL log record a clean shutdown.
4. Ran the **same command** again. `GET /api/v1/main/executions/5F7VtHUdjnNoJk7xeHofZH` returned the same execution still `PAUSED` with the same task states.
5. `POST /api/v1/main/executions/5F7VtHUdjnNoJk7xeHofZH/actions/resume` returned HTTP 200. Later `GET` returned whole execution `SUCCESS`, with `before`, `approval`, and `after` all `SUCCESS`.
6. Stopped the launcher cleanly; no listener remained on PostgreSQL port 25436 or Kestra API port 28087.

This verifies durability across a full process stop/restart on the same host with the copied local PostgreSQL backend, not H2. The [live 2.0.3 OpenAPI](https://kestra.io/docs/api-reference) informed the `actions/resume` call; the response and state files are scratch-only because they include instance-specific operational data.

## Limits before a product one-install claim

- **Portability:** Only one current macOS arm64 machine was tested. The copied Homebrew bottle was built for Darwin 25.6.0. Minimum supported macOS version, other arm64 hosts, x86_64, Linux/Windows, offline first-run, system library compatibility, and path relocation to another install location are not established. The runtime still reports PostgreSQL as `(Homebrew)` because that is its build provenance.
- **Distribution:** `install_name_tool` mutation required local ad hoc `codesign`. A product package needs a reproducible source/artifact chain, proper signing/notarization if distributed for macOS, license/NOTICE assembly for the JDK, Kestra JAR and 18 copied dylibs, and resolution of the shaded `isorelax` uncertainty identified in [S0](../result.md). No legal clearance is asserted.
- **Lifecycle:** `start.sh` is a bounded test launcher. Fixed ports and one data path would collide across multiple harness instances. It does not provide installation/uninstallation, automatic port allocation, OS service supervision, safe database migrations/upgrades, backup/restore, crash recovery policy, log rotation, or isolated per-instance identity management. The local Basic Auth/password and SCRAM setup is only a trial configuration, not a completed product security design.
- **Sizing:** The 609 MiB bundle includes a 66 MiB tiny database. It is not a footprint under representative workloads. The earlier [S0](../result.md) observed roughly 650 MiB Kestra RSS at warm idle with `-Xmx1g` and four worker threads; resource targets still need a product envelope.

**Stop decision:** Kestra survives the bounded “can the harness start its own durable Docker-free helpers?” test on this host. No further S0 runtime work is necessary for the engine choice. Treat the product one-install claim as **unproved** until a clean-machine distributable package and license closure exist.
