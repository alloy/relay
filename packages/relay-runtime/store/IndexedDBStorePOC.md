# Relay Store IndexedDB POC Plan

## Goal

Create a proof-of-concept path to replace the Relay store's in-memory record source with IndexedDB-backed storage, optimized for:

- Fast read latency
- Low JS heap memory usage
- Better behavior than "IDB + always materialize full in-memory source"

## Important constraint discovered in current code

`RelayModernStore` currently depends on a synchronous `MutableRecordSource` API (`get`, `set`, `delete`, `getRecordIDs`, etc). IndexedDB is asynchronous.

This means a full production replacement requires either:

1. A store API evolution to support async reads, **or**
2. A synchronous hot cache layer in front of IndexedDB with explicit hydration semantics.

For this POC we focus on benchmarked design choices and a migration path with minimal risk.

## Design choices for POC

Based on IndexedDB guidance (including RxDB's slow IndexedDB recommendations), this POC prioritizes:

1. **Single write transaction per normalized payload publish**  
   Avoid one transaction per record.
2. **Batched reads for cold misses**  
   Fetch many records in one readonly transaction.
3. **Small hot cache over full in-memory mirror**  
   Keep only frequently-read records in memory.
4. **Avoid eager full materialization from IDB**  
   This is the memory-heavy baseline to compare against.

## Optimistic updates strategy

Optimistic updates should remain in-memory layered state for now (same model as current `RelayOptimisticRecordSource`), because:

- IDB transactions are short-lived and auto-commit quickly.
- Aborting an IndexedDB transaction only works while it is active.
- "Abort older layer but keep newer layer" semantics map naturally to explicit in-memory layers, not long-lived DB transactions.

### Experiment to run later

If we attempt IDB-backed optimistic layers:

- Persist each optimistic layer under its own layer ID.
- Resolve effective record by newest-layer-wins merge.
- "Abort layer" = delete layer entries in a single transaction.
- This avoids coupling correctness to the abort behavior of overlapping native transactions.

## Benchmark matrix (implemented in `benchmarks/indexeddb-relay-store-poc.html`)

The benchmark compares:

1. **In-memory only** (best-case speed, worst memory for large datasets)
2. **IDB + full materialization** (persist + duplicate memory source)
3. **IDB direct with bounded hot cache** (target architecture)

### Measurements

- Bulk write time (ms)
- Cold read time (ms)
- Warm read time (ms)
- Heap usage (if browser exposes `performance.memory`)
- Cache sizes / record counts as memory proxy

## How to run benchmark

From repository root:

```bash
cd /home/runner/work/relay/relay
python3 -m http.server 8000
```

Then open:

`http://localhost:8000/packages/relay-runtime/store/benchmarks/indexeddb-relay-store-poc.html`

Adjust dataset size and hot-cache size in UI and run multiple times.

## Expected decision criteria

Select architecture that:

1. Keeps warm reads close to in-memory baseline for hot data.
2. Has significantly lower JS heap growth than full materialization.
3. Preserves stable write throughput with single-transaction bulk writes.

## Proposed incremental implementation path in codebase

1. Introduce an experimental IndexedDB-backed record source adapter behind feature flag.
2. Keep optimistic layer in memory.
3. Add explicit prefetch/hydration for critical operation roots.
4. Validate correctness with existing store tests + targeted new tests.
5. Roll out in experiments before default path.
