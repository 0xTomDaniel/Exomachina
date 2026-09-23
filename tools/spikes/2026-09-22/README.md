# 22 September 2026 factory decision evidence

These are bounded development-spike fixtures and sanitized observations for the [decision report](../../../docs/spike-results-2026-09-22.md). They are **not** Exomachina product code, an installer, production configuration, a load test or a legal clearance. Each result describes its actual runtime, fixture, counts and limits.

| Directory | Contents |
| --- | --- |
| [S0 Conductor](s0/conductor/result.md) | Pinned published POMs and stock-artifact eligibility finding; no runtime test. |
| [S0 Kestra](s0/kestra/result.md) | Native local-runtime result, YAML fixtures, release/dependency license inventory, bounded POM audit and [host-local bundle trial](s0/kestra/package/result.md). |
| [S1 Kestra](s1/result.md) | Native `Log → Pause → Log` restart and contested-state result, scripts and machine records. |
| [S2 harness](s2/) | Strands harness and A2A fixture evidence, including the bounded Kestra integration when complete. |
| [S3 publication](s3/) | Native YAML publication, child-revision and review-bypass evidence. |
| [S4 remote recovery](s4/result.md) | Custom HTTP transaction-gap fixture, candidate-backed continuation and combined Kestra/Strands A2A gate. |
| [Dagu countertrial](countertrials/dagu/result.md) | Released v2.17.0 local helper footprint, nested approval/restart, concurrent approval, and mutable child-version finding. |
| [Temporal countertrial](countertrials/temporal/result.md) | Non-development v1.32.0/PostgreSQL/Zigflow topology, pinned worker behavior, restart, publication gate and helper resource observations. |
| [Core Strands Graph countertrial](countertrials/strands_graph/result.md) | Pinned SDK graph publication fixture, snapshot/restart, minimal process footprint and unfenced same-run duplicate delivery. |
| [Effect Workflow + Cluster countertrial](countertrials/effect_workflow/result.md) | Pinned v3 SQL-backed SQLite single runner, approval wait across SIGKILL/restart, narrow duplicate-approval observation and helper-only resource measurement. |

The initial S0–S4 scratch workspace was `/tmp/exomachina-spikes/`; the later countertrials used `/tmp/exomachina-countertrials/`. These contain downloaded binaries, virtual environments, temporary credentials, PostgreSQL databases and logs; those are deliberately excluded here. Some scripts point at those scratch layouts and require fresh isolated configuration, pinned released artifacts and test credentials to rerun. Do not treat a fixture token, local Basic Auth or loopback PostgreSQL setting as product security. Result JSON records the observed run; YAML and scripts establish the tested shape. Primary upstream source URLs and exact release pins are in the corresponding result files. The countertrials ran concurrently on one host with different fixtures, so their resource figures are **not** a matched whole-product comparison.
