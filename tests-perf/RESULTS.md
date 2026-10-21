# Results

rhel-lat (Dell R640, Xeon Gold 6244, isolated CPUs 5-15 on node 1), JDK 27, 4 GB heap, 2026-10-20.
Medians of 2 runs per cell (`perf.sh v2`, `res`, `raw2`, `rawres`, `work40`); latency in microseconds, p50 / p99.
Run-to-run variation between sessions at 5 MT/s is about ±10-20%, so differences below that are noise.

## Latency, plain transport (`perf.work=0`)

| Scenario | MT/s | Revelator | Disruptor | raw |
|---|---|---|---|---|
| UNICAST | 1 | 0.20 / 0.24 | 0.20 / 0.24 | 0.23 / 0.27 |
| UNICAST | 10 | 0.27 / 0.46 | 0.36 / 1.19 | 0.22 / 0.34 |
| PIPELINE3 | 1 | 0.43 / 0.56 | 0.42 / 0.57 | 0.44 / 0.51 |
| PIPELINE3 | 10 | 0.54 / 0.94 | 0.59 / 1.58 | 0.63 / 1.37 |
| PIPELINE3_1T | 1 | 0.21 / 0.28 | 0.20 / 0.29 | 0.24 / 0.28 |
| PIPELINE3_1T | 10 | 0.35 / 0.89 | 0.35 / 1.16 | 0.35 / 0.81 |
| MULTICAST3 | 1 | 0.23 / 0.36 | 0.22 / 0.36 | 0.27 / 0.32 |
| MULTICAST3 | 10 | 0.67 / 2.58 | 17 / 88 | 2.3 / 12 |
| DIAMOND | 1 | 0.37 / 0.48 | 0.38 / 0.50 | 0.38 / 0.45 |
| DIAMOND | 10 | 0.65 / 1.77 | 67 / 249 | 0.97 / 2.90 |
| SHARDED4 | 1 | 0.35 / 0.53 | 0.46 / 0.61 | 0.39 / 0.47 |
| SHARDED4 | 5 | 0.46 / 0.77 | 0.63 / 1.06 | 0.46 / 0.75 |

SHARDED4 saturates at about 8-10 MT/s in all three implementations.

## Latency with stage results (`perf.results=true`)

Disruptor writes results into the event, Revelator and raw into a column per stage.

| Scenario | MT/s | Revelator | Disruptor | raw |
|---|---|---|---|---|
| PIPELINE3 | 1 | 0.43 / 0.63 | 0.66 / 0.75 | 0.46 / 0.59 |
| DIAMOND | 1 | 0.34 / 0.47 | 0.54 / 0.63 | 0.37 / 0.47 |
| MULTICAST3 | 5 | 0.23 / 0.44 | 0.41 / 0.72 | 0.25 / 0.45 |
| SHARDED4 | 1 | 0.45 / 0.67 | 0.56 / 0.69 | 0.43 / 0.53 |

## Burst throughput, MT/s

Not a capacity measure yet: it depends on whether consumers catch up with the publisher (see below), and some
cells flip between ~20 and ~120 MT/s from one session to the next.

| Scenario | Revelator | Disruptor | raw |
|---|---|---|---|
| UNICAST | 121 | 81 | 146 |
| PIPELINE3 | 118 | 20 | 150 |
| PIPELINE3_1T | 22 | 118 | 93 |
| MULTICAST3 | 120 | 13 | 148 |
| DIAMOND | 16 | 12 | 12 |
| SHARDED4 | 8.5 | 8.8 | 9.5 |
| PIPELINE3, results | 51 | 90 | 111 |
| DIAMOND, results | 58 | 86 | 96 |
| MULTICAST3, results | 81 | 118 | 141 |
| PIPELINE3, work=40 | 72 | 72 | 86 |

## Findings

- At low load all three are within noise for simple shapes: one cross-core hop costs about 200 ns here.
- SHARDED4: waiting only for the owner shard (Revelator, raw) is 20-25% faster than Disruptor's minimum over all
  shards at 1-5 MT/s.
- Near 10 MT/s Disruptor falls apart on MULTICAST3 and DIAMOND while Revelator stays below 3 us p99.
- Stage results in a single-writer column instead of the shared event: 20-40% lower latency (Revelator, raw vs
  Disruptor). Dense columns (raw, indexed by sequence) also give the highest throughput; Revelator's columns are
  indexed by message offset, one result per ~80 bytes, which costs throughput.
- PipelinedFlowProcessor (PIPELINE3_1T) tops out at ~22 MT/s while a fused handler does ~100; its latency matches.
  Fusing three cheap stages on one thread halves latency against three threads (0.21 vs 0.43 us).
- More work per stage can raise throughput (results mode, `perf.work=40`): when consumers are as fast as the
  publisher they spin on its cursor, which it writes for every message, and each publish then has to take the line
  back from all spinning readers. SHARDED4 stays at ~8-10 MT/s even with work because shards skip 3 of 4 messages and
  always catch up. Candidates: header-commit publication, batched publish, reader back-off.
- The first raw version was slower than both frameworks: the publisher wrote a counter on every message next to the
  fields consumers read (false sharing). Fixed by moving publisher state into its own padded object.
