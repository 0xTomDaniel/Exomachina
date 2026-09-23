# S0 result — Conductor v3.32.4 local package gate

> **Subsequent finding:** the [follow-up investigation](../../../../../docs/conductor-license-follow-up-2026-09-22.md) built and ran a PostgreSQL-only source variant without the restricted queue JAR. The stock artifact gate below remains; the earlier statement that no source variant had been built is now superseded.

**Result:** **Blocked for the stock Conductor distribution.** The published `conductor-server:3.32.4` boot JAR physically contains `orkes-conductor-queues:2.0.0.rc3`, whose published license is the Orkes Community License. That license contains Article 3.3 restrictions on third-party service and competing-product use. Exomachina's intended open-source/commercial, customer- and agent-authored factory product does not have established rights for this dependency. The Apache 2.0 license of Conductor's core does not clear the assembled artifact. This is an artifact eligibility gate, not a finding that PostgreSQL operation is impossible or that Conductor's execution is defective. The exact legal fit would require clearance from the rights holder or a product counsel determination; no such clearance is established here.

**Stop decision:** Do not run downstream Conductor runtime spikes against the stock binary as evidence for the intended distributable product. An Apache-only PostgreSQL assembly is *plausible from source*, but it has not been built, dependency-audited, booted or tested. It is a conditional route only if the team accepts maintaining and verifying that package; this S0 did not turn dependency removal into a fork project. The other route is explicit rights covering the actual Exomachina use and distribution. If neither route is acceptable, compare a locally runnable candidate with a cleared package.

## Reproducible evidence

| Check | Command / observation |
| --- | --- |
| Source pin | `git clone --depth 1 --branch v3.32.4 --filter=blob:none https://github.com/conductor-oss/conductor.git conductor-v3.32.4`; `git rev-parse HEAD` → `088de237a185caaeb669035a6d75de65d36cab5a`. Source is under this S0 directory. |
| Published artifact | `curl -I https://repo.maven.apache.org/maven2/org/conductoross/conductor-server/3.32.4/conductor-server-3.32.4-boot.jar` → HTTP 200, `content-length: 457760871` bytes (436.6 MiB). Maven's `.sha256` → `63da7d575504ad9545c8b3514443f14dc0e84b70b2cf94f0c06a720cde5e83b1`. This digest is publisher metadata; the complete 457 MB JAR was not downloaded or independently hashed. |
| Binary contents | `curl -fsSL --range -8388608 [boot JAR URL] -o conductor-boot-tail.bin`; `rg -a -o 'BOOT-INF/lib/orkes-conductor-queues[^[:cntrl:]]*' conductor-boot-tail.bin` → `BOOT-INF/lib/orkes-conductor-queues-2.0.0.rc3.jarPK`. This directly locates the library in the boot JAR ZIP entry directory without downloading the full JAR. |
| Published dependency | The exact [server POM](https://repo.maven.apache.org/maven2/org/conductoross/conductor-server/3.32.4/conductor-server-3.32.4.pom) declares `io.orkes.queues:orkes-conductor-queues:2.0.0.rc3` as a runtime dependency. `server/build.gradle:106` declares it directly; `dependencies.gradle:66` pins the version. |
| License | The [queue POM](https://repo.maven.apache.org/maven2/io/orkes/queues/orkes-conductor-queues/2.0.0.rc3/orkes-conductor-queues-2.0.0.rc3.pom) names **Orkes Community License**, linking to its [terms](https://github.com/orkes-io/licenses/blob/14cd5d6b0619399c205a022cc7b512734ea51911/community/LICENSE.txt). Article 2.1 permits evaluation in development/testing subject to Article 3; Article 3.3 restricts third-party service and competitive-product uses. The source repository's [core license](https://github.com/conductor-oss/conductor/blob/v3.32.4/LICENSE) is Apache 2.0. |
| Multiple paths | The [server build](https://github.com/conductor-oss/conductor/blob/v3.32.4/server/build.gradle) also includes `conductor-redis-persistence`, `conductor-workflow-event-listener`, and `conductor-scheduler-redis-persistence`. The first two directly depend on the queue artifact; the scheduler module depends on Redis persistence. `conductor-task-status-listener` also depends on Redis persistence. Removing the server's single direct dependency is insufficient. These paths are visible in pinned Gradle files and in the published Redis/listener POMs. |
| PostgreSQL path | The pinned `docker/server/config/config-postgres.properties` sets database, queue and indexing to `postgres`; `postgres-persistence/src/main/java/com/netflix/conductor/postgres/dao/PostgresQueueDAO.java` implements that queue path. The [production guide](https://conductor-oss.github.io/conductor/devguide/running/deploy.html) calls the PostgreSQL database plus queue the simplest production stack. Selecting it in configuration does **not** remove the Orkes JAR from the stock distribution. |
| Native host | macOS arm64; `java -version` reports no installed runtime. `psql --version` is PostgreSQL 16.3, with local `postgres` and `initdb` present. The [native source guide](https://conductor-oss.github.io/conductor/devguide/running/source.html) requires JDK 21+ and notes default in-memory persistence is development-only. Docker is optional for tests in that guide; a native JAR launch is documented. |

Local evidence copies: `conductor-server-3.32.4.pom`, `orkes-conductor-queues-2.0.0.rc3.pom`, `orkes-community-license.txt`, `conductor-boot-tail.bin`, and the pinned source clone in this S0 directory. The suffix file is only a fragment of the JAR, not a runnable artifact.

## S0 pass criteria status

| Criterion | Status |
| --- | --- |
| Permitted artifact for intended product | **Blocked for stock boot JAR** by unresolved restricted dependency. Apache-only variant or explicit rights unproved. |
| Native one-command managed local package | **Unproved.** Upstream documents native JAR execution but a product launcher, bundled JRE and managed PostgreSQL lifecycle have not been assembled. |
| Native workflow execution and durable restart | **Unrun.** No product eligible artifact was available; local JDK is absent. Default upstream development mode is in-memory and cannot stand in for this criterion. |
| Startup/idle memory and disk | **Unmeasured.** Published stock boot JAR size is 436.6 MiB; it is not a runtime footprint measurement. |
| Two harnesses sharing helper lifecycle | **Unrun.** Requires the permitted local package and S2 harness fixture. |
| PostgreSQL queue/index/storage/lock providers | **Source/documentation path only.** Configuration suggests the path; loaded providers and behavior were not observed. |

## Uncertainties and next bounded decision

1. Whether a small source variant can omit every restricted runtime path while retaining needed PostgreSQL functionality, without sustaining a fork or losing required features. This requires a JDK 21 build, resolved runtime dependency report, boot, a native workflow and restart. Only undertake it if maintaining such an assembly is acceptable.
2. Whether explicit Orkes terms permit the precise embedded/distributed Exomachina product. No rights were requested or assumed.
3. Whether the self-contained launcher and managed storage meet an acceptable startup, idle memory, disk and upgrade footprint. There is no agreed numeric threshold yet, and this gate prevented measurement.
4. Other transitive license constraints were not exhaustively audited; this one confirmed restriction is sufficient to stop the stock artifact.

No Docker container, sandbox, Conductor engine process, workflow, runtime race test, or license-key bypass was run in S0.
