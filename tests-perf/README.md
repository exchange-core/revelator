# tests-perf

Revelator vs LMAX Disruptor 4.0.0 on the same pipeline shapes. One framework per JVM, results appended to a CSV.

## Scenarios

Every scenario fits into 6 isolated cores: one publisher plus at most 5 consumer threads.

| Scenario | Shape | Consumer threads |
|---|---|---|
| `UNICAST` | P → A | 1 |
| `PIPELINE3` | P → A → B → C | 3 |
| `MULTICAST3` | P → {A, B, C}, each sees every message | 3 |
| `DIAMOND` | P → {A, B} → C, C waits for both | 3 |
| `SHARDED4` | P → {S0..S3} by key % 4 → J | 5 |

In `SHARDED4` the Revelator join waits only for the shard that owns the message (conditional fence with cached
bounds); a Disruptor barrier can only wait for the minimum over all four shards.

Each message is a timestamp plus 6 longs. Every stage sums the payload (checked against the publisher at the end) and
can do `perf.work` extra multiply-adds. Terminal stages record the latency of every 16th message.

## Equal conditions

- Same JVM, flags, heap and NUMA node; the JVM itself runs on the housekeeping CPUs (`taskset -c 1,3`).
- Consumer threads come from one `PinnedThreadFactory`: the n-th thread of both frameworks lands on the same CPU,
  the publisher on the first CPU of the list.
- Disruptor: `ProducerType.SINGLE`, `BusySpinWaitStrategy`, preallocated events with primitive fields,
  `BatchEventProcessor` per stage. Revelator also busy-spins.
- About the same ring memory: Disruptor `perf.ringMsgs` entries, Revelator `perf.ringMsgs * 8` longs.
- Every handler is one class per framework, so the handler call sites stay monomorphic.
- The publisher loop runs in chunks of 4096 messages from a separate method (normal C2 compilation, not OSR),
  identical code for both frameworks; 3 warm-up rounds before measuring.
- Open-loop latency: message j is due at t0 + j/rate and carries the due time, so a publisher falling behind
  appears as latency (no coordinated omission).
- The log reports GCs and JIT compilation time inside the measurement window.

Known asymmetries left as they are (they are what is being compared): a full Disruptor ring parks the publisher
with `parkNanos(1)` (~50 us), Revelator spins; Revelator releases ring space every 4096 longs, Disruptor at the end
of a batch (`perf.maxBatch` bounds it).

## Running on the test server

```
mvn -DskipTests package -pl tests-perf -am
mvn dependency:copy-dependencies -pl tests-perf -DoutputDirectory=target/lib -DincludeScope=runtime
# copy tests-perf/target/lib/*.jar and the tests-perf jar to ~/rev2/lib, then:
./perf.sh <label> <repeats> [scenarios...]     # results/<label>/results.csv, summary.md
```

Single run: `java --enable-native-access=ALL-UNNAMED -Dperf.impl=disruptor -Dperf.scenario=DIAMOND -cp "lib/*" exchange.core2.revelator.perf.PerfMain`.
All settings are listed in `PerfMain`.
