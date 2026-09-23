# Effect footprint while external A2A assignments are in flight

23 September 2026. This bounded follow-up measures the active assignment phase against the [Dagu in-flight trial](../dagu-inflight-scale/result.md). It uses the existing Effect graph helper, bridge, definition, and Director facade without editing them; `freeze.py verify` passed for the five recorded helper, bridge, definition, and package files with inventory digest `3d051010b0cccb006f1185e6982216249230efae11675635008bdef70b2c9d21`. The separate source Strands/A2A fixture is copied into this directory and holds each source Task in `working` behind the same bounded gate behavior used for Dagu. The counter, independent Quality, and release services are the existing fixtures.

[`inflight_probe.py`](inflight_probe.py) publishes the existing v3 graph package and admits each parent via the original Director A2A `message/send` route. The parent invokes the pinned child; the child starts source and counter in parallel. At each 2/10 sample, every original Director A2A Task was `working`, each child was in its native Effect `parallel` phase, and every separate source A2A Task was `working` under the expected child action ID. The probe checked those identities and states again after sampling. [`observed.json`](observed.json) preserves all Task IDs, run IDs, process IDs, roles, commands and footprint metadata. The package digest for this fresh trial was `0409b9e601f98a18faf6639cfc21b4c413a03d79267c7ffe10a8c138e6730e35`.

| In-flight source assignments | Complete selected local bundle | PIDs | Added role beyond six base PIDs |
| ---: | ---: | ---: | --- |
| 0 | 596,648,464 B | 6 | None |
| 2 | 641,427,160 B | 8 | 2 Python `bridge.py assign` processes |
| 10 | 803,632,960 B | 16 | 10 Python `bridge.py assign` processes |

The six base processes were the Effect Node helper, Strands Director, two Strands/A2A capability services, independent Strands/A2A Quality service, and participating release receiver. The probe selected those roots and all descendants using the same macOS `footprint -f bytes --noCategories` method as Dagu. It did not include a production supervisor, opaque release receiver, or separate reconciler; Dagu's complete trial bundle did include those three roles and had a nine-process baseline. The two- and ten-task admission-to-sample stages took 0.53 and 1.22 seconds. Both trials are fresh serial runs on the same Mac, with the same two-branch graph shape and held source Task, but different built-in runtimes and bundle plumbing. These are snapshots of physical footprint, not peaks or a scaling law.

The directly observed contrast is in the **increment during these held calls**. From its own baseline to ten working assignments, this Effect bundle added ten Python bridge PIDs and 206,984,496 B. The Dagu bundle added ten Dagu runners, ten Python adapters, ten shell watchers, and 729,612,416 B. At ten, their whole selected bundles were about 804 MB/16 PIDs and 1,278 MB/39 PIDs respectively. The absolute gap includes the three different baseline roles, while the added process topology follows the two implementations' observed call paths. Effect's current bridge is still a product-owned Python subprocess per A2A call; this trial does **not** show a processless Effect call path. A native asynchronous Node adapter could change that cost, but has not been implemented or tested. Dagu likewise has a plausible native gate plus shared reconciliation adaptation that has not yet been implemented for assignments.

One limit is the fixture's long-running `message/send`: a production A2A service may return a `working` Task immediately, shifting the retained work into polling or push reconciliation. Neither candidate has a tested production-grade long-running A2A contract, and this result does not prove either engine's worst-case footprint. The held source fixture has a 120-second safety timeout; the two runs completed in roughly 14 seconds and all in-flight Task states were verified before shutdown.

Reproduce from the repository root with fresh `/tmp` state:

```sh
tools/spikes/2026-09-22/s2/.venv/bin/python -B tools/spikes/2026-09-23/effect-inflight-scale/inflight_probe.py
```
