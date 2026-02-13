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

## Detailed implementation plan

### Phase 0: Scope, flags, and guardrails

1. Add an experiment flag in `RelayFeatureFlags` to opt into IndexedDB backing in browser-only environments.
2. Keep default behavior unchanged (`RelayRecordSource` in-memory).
3. Ensure all new IndexedDB code paths no-op or fall back safely when `indexedDB` is unavailable.

### Phase 1: Introduce experimental IndexedDB-backed source

Target files (new):

- `packages/relay-runtime/store/RelayIndexedDBRecordSource.js`
- `packages/relay-runtime/store/__tests__/RelayIndexedDBRecordSource-test.js`

Target files (minimal updates):

- `packages/relay-runtime/store/RelayStoreTypes.js` (experimental helper typing only)
- `packages/relay-runtime/store/RelayFeatureFlags.js` (flag)

Implementation details:

1. Implement a bounded LRU hot cache (`Map`-based) for synchronous `get` reads.
2. Persist canonical records in IndexedDB object store keyed by `DataID`.
3. Preserve `MutableRecordSource` surface (`get`, `set`, `delete`, `remove`, `clear`, `has`, `getStatus`, `getRecordIDs`, `toJSON`) with:
   - synchronous reads from hot cache + known metadata
   - async flush queue that commits write batches in one `readwrite` transaction
4. Add explicit `prefetch(dataIDs)` helper (experimental) that warms hot cache via one `readonly` transaction.
5. Add explicit `flush()` helper (experimental) for deterministic test synchronization.

### Phase 2: Integrate with publish path (single transaction per normalized payload)

Target file:

- `packages/relay-runtime/store/RelayModernStore.js`

Implementation details:

1. Detect IndexedDB-backed source and route `publish` updates through a batched write method.
2. Ensure one publish call writes all changed normalized records in one IndexedDB transaction.
3. Keep `updatedRecordIDs` and invalidation behavior unchanged.
4. Retain current optimistic layering (`RelayOptimisticRecordSource`) over the base source.

### Phase 3: Environment wiring

Target file:

- `packages/relay-runtime/store/RelayModernEnvironment.js`

Implementation details:

1. Add experimental environment option (e.g. `indexedDBStorageConfig`) that instantiates the IndexedDB-backed source when enabled.
2. Fall back to existing in-memory source automatically on unsupported platforms or initialization failure.
3. Add instrumentation hooks (`log`) for source creation, flush latency, flush failures, prefetch latency.

### Phase 4: Correctness and regression tests

Test suites:

1. `RelayIndexedDBRecordSource-test.js`
   - set/get/delete/remove/clear/getStatus/size semantics parity
   - hot cache eviction behavior
   - flush error handling + recovery
2. `RelayModernStore-test.js` (targeted additions)
   - publish updates are visible through lookup with IndexedDB source
   - invalidation epoch semantics remain unchanged
   - snapshot/restore optimistic behavior remains unchanged
3. `RelayModernEnvironment` tests (targeted)
   - config gating and fallback behavior

### Phase 5: Benchmark extension and acceptance gates

Extend `benchmarks/indexeddb-relay-store-poc.html` with:

1. Operation-locality traces (high locality vs low locality).
2. Warmup/prefetch stage timing separate from steady-state warm reads.
3. Publish throughput benchmark that simulates normalized payload batch writes per operation.

Acceptance criteria for advancing past POC:

1. Warm read latency (with realistic locality + prefetch) within acceptable delta of materialized-memory baseline.
2. Heap growth materially lower than full materialization under same data volume.
3. Stable publish throughput and no correctness regressions in existing store tests.

### Phase 6: Rollout and risk management

1. Ship behind disabled-by-default flag.
2. Run benchmark and targeted integration tests in CI for the experiment path.
3. Collect metrics from internal adopters before considering broader enablement.
4. Define rollback path: disable flag and revert to in-memory source without data loss.
