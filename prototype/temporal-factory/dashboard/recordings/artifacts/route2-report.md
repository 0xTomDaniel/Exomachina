## Live-proven
All three spikes passed their pre-registered checks on the merged tree. The delayed-agent and two-instance tests used real processes but a fixture Director. Separately, a live broker-backed `gpt-6-sol` Director passed C-1–C-5: a text brief started a run, the Task reached `input-required`, and one caller-directed abort completed it with zero releases. Code rejected a forbidden input requested by the live model; stale and unauthorized probes were fixture-injected. [E3, E5, E8, E9, E10]

## Fixture-only
The delayed external agent and the Directors in the delayed-agent and two-instance tests were fixtures. Quality, capability content, and the HTTP release receiver remain fixtures. Their passing checks are not live proof of an end-to-end production path. [E2, E3, E4, E7]

## Remaining gaps
Live Director evidence covers one account, one model, and a caller-directed abort; reliability across broader briefs and autonomous decisions remains unmeasured. External-agent authentication, signed or attested cards, a general directory, and workflow-level polling for long remote Tasks are absent. Topology testing covers only two instances; embedded `create_app()` lacks an instance lock, and stress testing is absent. Old-build retirement, operational hardening, and the memory ceiling remain unresolved. [E6, E7]

## Next priority
Qualify an end-to-end path with real Quality, capability, and release components, then broaden live Director brief and decision testing. Develop authenticated external-agent contracts and durable remote-Task polling, while keeping fixture and live results explicitly distinct. [E2, E6, E7]