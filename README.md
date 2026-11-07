# Revelator framework

[![][license img]][license]

Revelator is a **low latency messages processing** framework in Java (experimental): one producer writes
variable-size messages into a ring of longs, processors read them in place.

### Features

* With the original Disruptor design, EventHandler is dealing with each incoming message independently by performing
complete piece work over it. With Revelator it is possible to easily handle different processing stages on the same
core, depending on how other cores are progressing (relying on relevant memory fences). This design allows to utilize L1
and L2 caches much more efficiently, and avoid unnecessary cache coherency traffic. It is also possible to do as much as
possible work when receiving batches of messages - Revelator operates as a Kanban-style processor - refer to the
PipelinedFlowProcessor.java for more details.

* To combine N multiple fences into a single sequential constraint, Disruptor only has a min-aggregation rule
(SequenceGroup.java), where the handler always makes N volatile reads and then processes all available messages up to
a less progressing predecessor. There is no simple way to conditionally wait for specific sequence barriers. For
latency-critical services, Revelator offers a more flexible design to improve mean latency - it is possible to define a
function that maps message properties (such as account id) into a relevant fence, therefore avoiding unnecessary
volatile reads.

* In the Disruptor - message handlers are not batch-aware. There is no way to break a large spike batch into smaller
pieces, so that the follower can start catching up earlier, before the batch is completed. The Revelator offers such
functionality to provide optimal balance between latency and throughput.

* Messages that do not share keys with a message stuck behind a slow dependency can go ahead of it
(ReorderingFlowProcessor, experimental).

* Readers can wait on commit words in the message headers instead of a shared cursor (HEADER_COMMIT, experimental).

### Modules

| Module | Contents |
|---|---|
| `revelator-core` | ring, fences, flow processors (simple, pipelined, reordering) |
| `revelator-examples` | BasicExample, CooperativeExample |
| `tests-common` | latency measurement helpers |
| `tests-perf` | Revelator vs LMAX Disruptor benchmarks, see [tests-perf/README.md](tests-perf/README.md) |

RAFT, the scalable processors and the old payments/primes examples were removed in 0.5, see
[docs/HISTORY.md](docs/HISTORY.md).

### Installation

Java 22 or newer: the thread pinning dependency `collections-affinity` uses the FFM API. It is not released yet,
install it from [exchange-collections](https://github.com/exchange-core/exchange-collections) (`mvn install`) first.

1. Install the modules into your Maven's local repository by running `mvn install`
2. Add the following Maven dependency to your project's `pom.xml`:

```
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>revelator-core</artifactId>
    <version>0.5.0-SNAPSHOT</version>
</dependency>
```

Pinning needs `--enable-native-access=ALL-UNNAMED`.

### Usage

```java
long[] sum = {0};
try (Revelator r = Revelator.create(1024,
        ProcessorsFactories.single((buffer, index, size, timestamp, position, id, type) -> sum[0] += buffer[index]),
        Thread::new)) {
    r.start();
    long payload = r.claimSingleMessage(1, System.nanoTime(), 1, (byte) 1);
    r.writeLongData(payload, 0, 42);
    r.publish(payload + 1);
}
// close() has drained the ring and joined the processor thread: sum[0] == 42
```

Sizes and positions are in longs. One thread claims, writes and publishes; a claim has to be filled before the next
one. Handlers must not keep references to the buffer after they return. A handler exception stops the whole ring
(`state()` becomes FAILED, `failure()` holds the cause). Stop the producer and publish its last batch before
`close()`. The full contract is in [docs/API.md](docs/API.md), changes in [RELEASE_NOTES.md](RELEASE_NOTES.md).

### Contributing

Revelator is an open-source project and contributions are welcome!

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
