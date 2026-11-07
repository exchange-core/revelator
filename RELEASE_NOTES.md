# 0.5.0-SNAPSHOT

* Java 22 is the minimum: collections-affinity uses the final FFM API (class files major 66). No preview flags.
* Removed revelator-raft (and the Chronicle dependency), the scalable processors and the old payments/primes
  examples; where to find them is in docs/HISTORY.md. revelator-examples has two small programs instead.
* Lifecycle: NEW -> RUNNING -> STOPPING -> TERMINATED, or FAILED. `close()` drains and joins (it used to do
  nothing); `stopAsync(Duration)` returns at once and fails after the timeout; a handler exception stops the
  whole ring instead of being logged and skipped; claims and publishes after stop are rejected.
* `publish(end)` takes the end of any claimed message (a prefix of the batch), not an arbitrary position.
* HEADER_COMMIT (experimental): the commit word is the position XOR-ed with a random tag of the ring instead of a
  constant, so payload left from the previous lap cannot be made to look like a commit (2^-64 by chance). Padding
  is published right away, which removes a deadlock when a large message follows padding.
* PipelinedFlowProcessor rewritten: the shutdown message is no longer handed to stages, sessions get
  `frameStart`/`endExclusive` and a `reset()` hook, stages get `onShutdown()`, and progress is published when the
  processor has caught up with the producer or every 4096 longs (it used to be published only when all 64 sessions
  were done).
* ReorderingFlowProcessor: a barrier (signature -1) is no longer passed by messages with signature 0 and does
  not pass them.
* Removed ExceptionHandler, MSG_TYPE_TEST_CONTROL handling, `getHitWorkWeight`, the session `globalOffset` and
  `AFFINITY_LOGICAL_CORE` (it pinned whole cores anyway).

Measured against a7adc42 on rhel-lat: tests-perf/RESULTS.md, "0.5 development".

Before a release: release collections-affinity (it was not part of collections 0.6.0), run the tests on JDK 22
and 25, see docs/RELEASE.md.
