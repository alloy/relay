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

## Benchmark runs (2026-02-13)

Collected from `benchmarks/indexeddb-relay-store-poc.html` in browser:

### Run A: recordCount=20000, readSampleCount=4000, hotCacheSize=1000

- `in-memory-only`: write 3.9ms, cold 0.5ms, warm 0.2ms, heapDelta 0
- `idb-plus-materialized-memory`: write 1354.3ms, materialize 122.6ms, cold 0.3ms, warm 0.4ms, heapDelta 10,462,827
- `idb-direct-bounded-hot-cache`: write 1653.4ms, cold 121.2ms, warm 86.3ms, warmMisses 3000, heapDelta -5,481,314

### Run B: recordCount=20000, readSampleCount=4000, hotCacheSize=4000

- `in-memory-only`: write 4.2ms, cold 0.6ms, warm 0.2ms, heapDelta 0
- `idb-plus-materialized-memory`: write 1679.5ms, materialize 145.0ms, cold 0.5ms, warm 0.3ms, heapDelta 12,082,124
- `idb-direct-bounded-hot-cache`: write 1744.0ms, cold 104.4ms, warm 0.3ms, warmMisses 0, heapDelta 10,433,146

### Run C: recordCount=50000, readSampleCount=10000, hotCacheSize=2000

- `in-memory-only`: write 8.9ms, cold 1.4ms, warm 0.6ms, heapDelta 0
- `idb-plus-materialized-memory`: write 3848.2ms, materialize 324.6ms, cold 0.3ms, warm 0.3ms, heapDelta 19,191,675
- `idb-direct-bounded-hot-cache`: write 4274.8ms, cold 427.4ms, warm 273.7ms, warmMisses 8000, heapDelta 8,647,871

### Initial takeaways

1. Full materialization keeps read speed near in-memory baseline but increases JS heap sharply.
2. Direct IDB + bounded cache reduces memory pressure relative to full materialization, but read speed depends strongly on cache hit rate.
3. To reach "best read speed + low memory", the POC should combine bounded hot cache with query-root prefetch/hydration and read batching.

## Next implementation plan (full POC)

1. Add `RelayIndexedDBRecordSource` experiment behind a feature flag with:
   - bounded in-memory LRU hot cache
   - batched `publish` write API that commits a single readwrite transaction
   - batched read API for cold misses
2. Keep `RelayOptimisticRecordSource` unchanged and layered in-memory above the base source.
3. Add `RelayModernEnvironment` experiment option to opt into IDB source in browser.
4. Add tests for:
   - consistency between cache+IDB reads
   - deletion/remove semantics
   - publish transaction failure handling
   - optimistic snapshot/restore compatibility
5. Add benchmark extensions:
   - repeated operation-level read traces
   - varying cache sizes and working set locality
   - throughput of single-transaction publish for normalized payload batches
