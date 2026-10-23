# spikes

Code behind each phase write-up in [`docs/results/`](../docs/results/). Kept so the results can be reproduced. None of it is the langx API.

| Folder | Write-up | Contents |
| --- | --- | --- |
| [`phase0/`](phase0/) | [`phase0.md`](../docs/results/phase0.md) | spike harness: `_p0` bindings, root, workers, coordinator, island sweeps, spike D |
| [`phase1/`](phase1/) | [`phase1.md`](../docs/results/phase1.md) | `diag_survivor_fence.py`: a dead observer fences a surviving worker |
| [`phase2/`](phase2/) | [`phase2.md`](../docs/results/phase2.md) | `coordinator_loss.py`: workers after coordinator loss, and the worker restart path |

`phase1/` and `phase2/` use the langx package and the island launcher in [`tests/integration/island.py`](../tests/integration/island.py), so they need the build from the top-level README. Run them from the repository root, for example `python spikes/phase2/coordinator_loss.py --c1-exit kill`.
