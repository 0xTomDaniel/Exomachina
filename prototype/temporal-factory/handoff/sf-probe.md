# Pre-integration probe (not counted)

24 September 2026. This is a bounded role-behaviour probe, separate from the pre-registered integrated scenario. It uses the pinned qualification packet and its `default_question`. No service, role, packet, or stimulus implementation was changed.

## Commands and evidence

Working directory: `prototype/temporal-factory` in the `sf/agents` worktree. Python was `/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python`.

| Command/action | Result |
| --- | --- |
| `node broker/exo-model.mjs status` | Signed in, unexpired, account hash `sha256:188b022d6e97`. The same status was recorded before and after the live run. |
| Check the names `EXO_MODEL_HOME`, `EXO_CODEX_BASE_URL`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` in the environment | All unset; values were not inspected. |
| `$PY -B scenarios/sf_agent_probe.py --provider scripted` | Pass, observed-synthetic, 4.545 s; 8 completed Tasks, 8 scripted model calls, all four behavioural checks pass. |
| `$PY -B scenarios/sf_agent_probe.py --provider codex-subscription` | Pass, observed-real live `gpt-6-sol`, 94.806 s; 8 completed Tasks, 8 live calls in 8 distinct sessions, all four behavioural checks pass. This was the only live invocation. |
| Broker `leak-scan`, invoked by the probe through `live_authoring.positive_control` and `scan_paths` | Fresh synthetic control detected in both planted files. Scan of both probe homes, evidence, and default broker home: 0 real hits, including a final scan after writing the evidence; canonical credential file excluded by the broker scanner. |
| `git diff --check` | Pass. |

The driver uses `message/send` with one DataPart and `configuration.blocking:false`, then polls `tasks/get`. It starts findings, risks, synthesizer (`--test-controls`), and Quality as four separate `model_agent.py` processes with distinct identities and SQLite state directories. It joins the two research artifacts as `packet_evidence_join@1`. It saves each Task's identity, id, state, artifact binding, model-call count, model id, live flag, session id, outcome, and duration, plus every Quality verdict and finding.

- Script and JSON evidence: `scenarios/sf_agent_probe.py`, `evidence/single-factory/agent-probe-scripted-1.json`, `evidence/single-factory/agent-probe-live-1.json`.
- Live reports: `evidence/single-factory/agent-probe-live-1-clean-r1.md`, `agent-probe-live-1-planted-r1.md`, `agent-probe-live-1-repair-r2.md` in the same directory. Scripted reports use the same naming pattern with `scripted`.
- Preserved trial homes: `/tmp/exo-sf-probe-1`, `/tmp/exo-sf-probe-2`; synthetic leak control: `/tmp/exo-sf-probe-2-leak-control`.

## Live observations

| Step | Outcome | Model-call duration |
| --- | --- | ---: |
| Research findings | 5 packet-cited findings; completed | 10.221 s |
| Research risks | 5 packet-cited risks; completed | 11.489 s |
| Clean draft r1 | Report completed; Quality accepted with `decided_by:model`, no findings | Synthesis 17.438 s; Quality 5.542 s |
| Planted draft r1 | Exactly one induced claim, C6; Quality rejected with `decided_by:model` and a blocking C6 finding citing E6/E7 | Synthesis 15.823 s; Quality 7.928 s |
| Repair r2 | Planted text absent; Quality accepted with `decided_by:model`, no findings | Synthesis 13.442 s; Quality 5.711 s |

Every Task used one `codex-subscription` `gpt-6-sol` call with `live:true`; all calls completed. Total recorded call duration was 87.594 s, with median 10.855 s. The service's model-call ledger and broker event records do not expose token counts, so tokens per call are unavailable; the evidence records `tokens:null` rather than estimates.

## Role judgement and prompt recommendations

- **Research findings:** Sensible. It distinguished real process evidence from fixture Director evidence and cited packet IDs. Recommendation: ask it to label forward-looking priorities explicitly as recommendations, since F5 goes beyond a packet fact.
- **Research risks:** Sensible. It identified the remaining fixture, trust, coverage, topology, and operational gaps with citations. The proposed release-receiver integration is a future priority, outside this spike's fixture-release scope. Recommendation: ask it to preserve that scope distinction.
- **Synthesizer:** The clean report answered live versus fixture status, gaps, and next priority. Its repair removed C6's text and factual assertion. The repaired markdown omitted inline evidence markers that the clean markdown had, although its structured claims retained packet citations. Recommendation: require inline packet citations in each report section, including repairs.
- **Quality:** Appropriately selective in this probe: accepted the clean draft, caught the induced unsupported live-Quality claim by ID, and accepted the repaired report. No evidence of excessive strictness or leniency from these three reviews alone. Recommendation: if human-readable inline citations are required, add that to the rubric and prompt before judging future runs; the current rubric validates structured claim citations.

## Cleanup and limits

All eight service processes across the two runs were stopped; no listener remained on 45740–45759 after either run. The broker was left running. No commit, push, branch switch, deletion, login, refresh, or logout was performed. This probe did not exercise the factory, Director, Temporal workflows, release fixture, or the pre-registered integrated routes. The scripted result is synthetic; the live result covers the four agent roles only.
