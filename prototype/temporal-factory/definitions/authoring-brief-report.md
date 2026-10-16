# Author the verified report graph

Call `describe_vocabulary`, then submit a JSON template with exactly `schema`, `root`, `child`, and `run_inputs`. Validate the draft before submission and revise from the returned errors. Use only the approved service names in the vocabulary.

The root is `parent_verified_report`: `nested_factory` invokes digest-pinned child `verified_report`, then `complete`. Use `"@child"` for the draft child digest; materialization pins it. The child starts with a `parallel` node with two branches: `research_findings` (`packet_findings`, `packet_findings@1`, service `research_findings`) and `research_risks` (`packet_risks`, `packet_risks@1`, service `research_risks`). Both feed a `join` naming those branches. The join feeds `synthesize` with service `synthesizer`, followed by `quality` and a `route` over `verdict.accepted`.

The true route releases to service `release`, then completes. The false route enters `repair` with `max_repairs: 2`: its `next` returns to the same `synthesize` node, while its `exhausted` edge enters `director_wait` with `reason: "repair_exhausted"`, then `abort`. Use no other route fields or result types. The only run input is required caller string `question`, allowed to `fixture-operator`, with `may_affect_acceptance: false`.

The evidence packet is pinned by the operator when the template is materialized. Do not put packet content in the template. The factory sends packet-backed briefs to the bound agents; the graph does not create report content itself.
