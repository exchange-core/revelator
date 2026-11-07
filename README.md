# Revelator 0.5 (development)

A single-producer Java runtime for low-latency pipelines. The supported baseline is cursor publication
with packed frames. Handlers can run in a chain, in parallel, or as cooperative stages on one owner thread.

## Why Revelator

* Run several pipeline stages on one owner thread. While one operation waits for a dependency,
  the thread can prepare or advance other work while keeping owner-local state in cache.
* Check only the dependencies relevant to an operation, directly from application code. Cache attained
  bounds locally; keep readiness, the business decision and permission to reclaim memory distinct.
* Process available messages as batches and publish progress within a batch. Balance throughput and
  follower latency rather than making the entire input burst an indivisible unit.
* Experiment with key-based reordering while preserving conflicts and a safe reclamation prefix.

These are the design goals. Cooperative stages and experimental reordering exist; a reusable conditional
business gate and comprehensive metrics remain development work. The same-owner and selective-dependency
choices are the reason for this project, not a claim that every workload beats another runtime.

[Apache License 2.0](LICENSE).

## Java and dependencies

Java **22 or newer** is required. All default modules compile with `--release 22`. The affinity dependency
uses the final Foreign Function and Memory API and ships Java 22 bytecode (class major 66); Java 26/27 is
not required by the source/API baseline. Validation here uses JDK 26 to compile against the Java 22 API.
Runtime validation on a real JDK 22 is still required before tagging a release.

`collections-affinity:0.6.1-SNAPSHOT` must be available in the local Maven repository. It remains an
unreleased external dependency: publish/pin a released Java-22-compatible affinity artifact before
publishing Revelator 0.5. A matching external collections checkout can build/install affinity; do not
silently substitute a different snapshot version. A successful local build does not prove fresh-machine
availability of that dependency.

For native affinity operations use `--enable-native-access=ALL-UNNAMED` on the classpath. The basic examples
use ordinary threads and do not call affinity. Pinning is optional behavior; the existing adapter is kept
for compatibility. The adapter exposes physical-core pinning; there is no logical-core compatibility alias.

## Build

```sh
mvn verify
mvn install
```

Modules: `revelator-core`, `revelator-examples`, `tests-common`, `tests-perf`. RAFT has been removed.
Historical scalable/business experiments are referenced in docs/HISTORY.md rather than copied outside the build. Performance runs are manual, see tests-perf.

## Minimal usage

```java
long[] sum = {0};
try (Revelator r = Revelator.create(1024,
        ProcessorsFactories.single((b, i, n, ts, start, id, type) -> sum[0] += b[i]),
        Thread::new)) {
    r.start();
    long payload = r.claimSingleMessage(1, System.nanoTime(), 1, (byte) 1);
    r.writeLongData(payload, 0, 42);
    r.publish(payload + 1);
}
// close drained and joined the handler: sum[0] == 42
```

Complete runnable examples: BasicExample and CooperativeExample in `revelator-examples`.

## Contracts and operations

See [API contract](docs/API.md), [release notes](RELEASE_NOTES.md) and [release checklist](docs/RELEASE.md).
Only one producer may claim/write/publish. Fully fill a claim before making another claim: admission can
flush the earlier batch when the ring needs space. Publish a complete prefix by its payload end; publish the remaining batch before shutdown. All sizes
and positions use long words, not bytes. The input buffer is borrowed until the handler finishes.

Before shutdown, quiesce the producer and publish its last batch. `stopAsync(Duration)` returns immediately;
shutdown admission and joins run on a dedicated daemon coordinator with a deadline. `close()` waits for a
30-second drain. No new publication is accepted during/after shutdown. Worker exceptions fail the runtime,
interrupt peers and surface through shutdown; no failed command is silently skipped or retried. Handlers
must cooperate with interruption; applied effects are not rolled back. Never call close from a worker.

Busy spinning intentionally uses a CPU while idle. Observe `state()`, `failure()`, and `getTailStrike()`;
there is no automatic scaling, idle parking or comprehensive metrics service in this version.

HEADER_COMMIT is an experimental explicit PublicationMode opt-in. Successor invalidation protects
variable frames; look-ahead ownership is included in producer gating. Cursor remains the default.
LINE_ALIGNED remains experimental and depends on documented JVM/GC layout assumptions. Reordering is
experimental: signatures must include all conflicts, false must have no observable effects, and reclaim
progress remains a contiguous prefix rather than per-message completion.
