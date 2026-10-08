## Live-proven
All three spikes passed their pre-registered checks on the merged tree. Spikes A and B exercised real processes with a fixture Director; spike C also passed with a live broker-backed `gpt-6-sol` Director. In that live run, a brief reached an authorized start, its Task moved from `working` to `input-required`, code rejected a forbidden input, and one caller-directed abort was accepted with zero releases. The stale and unauthorized probes were fixture-injected, not spontaneous model behavior.

## Fixture-only
Quality, capability content, and the HTTP release receiver remain fixtures. The external agent was a local fixture process, and spikes A and B used a fixture Director. Passing those checks does not establish a live end-to-end release.

## Remaining gaps
Live Director evidence covers one account, one model, and abort only; autonomous decisions and reliability across broader briefs are unmeasured. External-agent signing or attestation, remote authentication, a general directory, and workflow-level polling remain unbuilt. Topology testing stops at two instances; embedded `create_app()` lacks an instance lock, and stress testing is outstanding. Old-build retirement, operational hardening, and the memory ceiling also remain unresolved.

## Next priority
Run an end-to-end qualification with real Quality, capability content, and release components, while expanding live Director decision coverage beyond caller-directed abort. Until then, do not treat passing fixture checks as production release proof.