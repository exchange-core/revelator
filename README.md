# Revelator framework

[![][license img]][license]

Revelator is a **low latency messages processing** framework in Java 26 (experimental).

### Features

* With the original Disruptor design, EventHandler is dealing with each incoming message independently by performing
complete piece work over it. With Revelator it is possible to easily handle different processing stages on the same
core, depending on how other cores are progressing (relying on relevant memory fences). This design allows to utilize L1
and L2 caches much more efficiently, and avoid unnecessary cache coherency traffic. It is also possible to do as much as
possible work when receiving batches of messages - Revelator operates as a Kanban-style processor - refer to the
PipelinedFlowProcessor.java for more details.

* A pool of fixed number of parallel message processors does not always work well. Volatile reads contention increases the
message processing latency. For latency-critical services, Revelator will offer a more efficient scaling design, where
more worker threads can facilitate spikes.

* To combine N multiple fences into a single sequential constraint, Diruptor only has a min-aggregation rule (
SequenceGroup.java), where the handler always makes N volatile reads and then processes all available messages up to a
less progressing predecessor. There is no simple way to conditionally wait for specific sequence barriers. For
latency-critical services, Revelator offers a more flexible design to improve mean latency - it is possible to define a
function that maps message properties (such as account id) into a relevant fence, therefore avoiding unnecessary
volatile reads.

* In the Disruptor - message handlers are not batch-aware. There is no way to break a large spike batch into smaller
pieces, so that the follower can start catching up earlier, before the batch is completed. The Revelator offers such
functionality to provide optimal balance between latency and throughput.

* Revelator will support extended metrics collection. It can be helpful when identifying bottlenecks of the processing
pipeline and measuring latency penalties in each processing stage.

### Modules

| Module | Contents |
|---|---|
| `revelator-core` | rings, fences, flow processors (simple, pipelined, scalable) |
| `revelator-raft` | RAFT consensus and replicated state machine (experimental) |
| `revelator-examples` | payments with two-phase transfers between account shards, primes pipeline |
| `tests-common` | latency measurement helpers |
| `tests-perf` | Revelator vs LMAX Disruptor benchmarks, see [tests-perf/README.md](tests-perf/README.md) |

### Installation

1. Install the modules into your Maven's local repository by running `mvn install`
2. Add the following Maven dependency to your project's `pom.xml`:

```
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>revelator-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Usage examples

See `revelator-examples`.

### Contributing

Revelator is an open-source project and contributions are welcome!

[license]:LICENSE

[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
