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
| PIPELINE3, work=40 (see note) | 72 | 72 | 86 |

Note: at that time `perf.work` was an LCG loop that the JIT folded (1000 steps cost ~30 ns), so the work=40 row ran
practically without stage work; its difference from the plain row is the regime variation described below, not
the effect of work. Stage work is now xorshift rounds, ~1.4 ns each.

The SHARDED4 latency rows above sampled only shard 0 messages (key % 16 == 0 implies key % 4 == 0). Shards are
symmetric there, so the numbers stand; the sampling now uses a key hash.

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
- More work per stage can raise throughput (results mode): when consumers are as fast as the
  publisher they spin on its cursor, which it writes for every message, and each publish then has to take the line
  back from all spinning readers. SHARDED4 stays at ~8-10 MT/s even with work because shards skip 3 of 4 messages and
  always catch up. Candidates: header-commit publication, batched publish, reader back-off.
- The first raw version was slower than both frameworks: the publisher wrote a counter on every message next to the
  fields consumers read (false sharing). Fixed by moving publisher state into its own padded object.

## Publication: cursor, batches, header-commit

`pubmatrix.sh pub` and `pub2` (2026-10-20, one session, one run per cell). `b16` = the publisher makes messages
visible every 16 messages and always before it waits for the schedule; `header` = commit word per message
(Revelator rows for `header` are from `pub2`, after the placeholder store into the commit word was removed).
Columns: p50 at 1 MT/s, p50 / p99 at 15 MT/s ("sat" = could only send that many MT/s), burst MT/s.

| Scenario | Config | 1 MT/s p50 | 15 MT/s | burst |
|---|---|---|---|---|
| UNICAST | Revelator cursor | 0.23 | 0.35 / 0.99 | 115 |
| | Revelator cursor b16 | 0.21 | 0.32 / 0.51 | 83 |
| | Revelator header | 0.18 | 0.33 / 0.80 | 83 |
| | Disruptor | 0.20 | 2.91 / 121 | 28 |
| | Disruptor b16 | 0.24 | 0.32 / 0.53 | 92 |
| | raw header | 0.14 | 0.25 / 0.52 | 120 |
| PIPELINE3 | Revelator cursor | 0.48 | 0.91 / 3.25 | 117 |
| | Revelator cursor b16 | 0.47 | 0.68 / 1.09 | 61 |
| | Disruptor | 0.46 | 79 / 235 | 15 |
| | Disruptor b16 | 0.50 | 0.62 / 1.52 | 48 |
| | raw header | 0.37 | 0.56 / 1.00 | 125 |
| MULTICAST3 | Revelator cursor | 0.26 | 573 / 4014 | 120 |
| | Revelator cursor b16 | 0.24 | 0.45 / 0.76 | 73 |
| | Revelator header | 0.20 | 1.05 / 4.38 | 75 |
| | Disruptor | 0.23 | sat 9.3 | 10 |
| | Disruptor b16 | 0.30 | 0.46 / 0.87 | 73 |
| | raw header | 0.18 | 0.46 / 1.50 | 117 |
| DIAMOND | Revelator cursor | 0.41 | sat 11.8 | 12 |
| | Revelator cursor b16 | 0.37 | 0.62 / 0.96 | 56 |
| | Revelator header | 0.33 | 40 / 179 | 79 |
| | Disruptor | 0.38 | sat 12.2 | 13 |
| | Disruptor b16 | 0.43 | 0.62 / 1.08 | 63 |
| | raw header | 0.30 | 0.66 / 1.45 | 144 |
| SHARDED4 | Revelator cursor | 0.39 | sat 8.0 | 8 |
| | Revelator cursor b16 | 0.38 | 0.82 / 1.27 | 49 |
| | Revelator header | 0.34 | sat 9.7 | 10 |
| | Disruptor | 0.46 | sat 8.9 | 9 |
| | Disruptor b16 | 0.48 | 0.85 / 1.34 | 52 |
| | raw header | 0.29 | 1.53 / 3.60 | 117 |

- Batches remove the cursor bottleneck in every implementation: SHARDED4, DIAMOND and MULTICAST3 hold 15 MT/s at
  p99 ~1-1.3 us instead of saturating at 8-12 MT/s, and latency at low load does not change (the publisher flushes
  before it waits). With batches Revelator and Disruptor are level on these shapes; the earlier "Disruptor collapses
  at 10 MT/s" was this cursor effect.
- Header-commit gives the lowest latency at low load (raw -20..-35% p50, Revelator -15..-20%) and, in raw, removes
  the bottleneck without batches. Revelator's header-commit still saturates SHARDED4 at ~10 MT/s and DIAMOND near
  15 MT/s - open: it parses headers during the scan and its 80-byte messages straddle cache lines; raw scans fixed
  64-byte slots.
- Burst throughput still depends on the regime (cursor b16 bursts are lower than cursor b1 in some shapes); use the
  15 MT/s column to compare capacity.

## Header-commit on SHARDED4 (2026-10-22)

`ab_hdr.sh`: capacity = highest load from a 4..30 MT/s sweep with p99 <= 10 us and the publisher keeping up.

| Variant | Capacity, MT/s (p99 there) | Burst, MT/s |
|---|---|---|
| before: header, batch 1 | 8-10 | 10 |
| before: header, batch 16 (forward commit) | 16 (3.1 us) | 19.6 |
| header, batch 1 | 8-10 | 10-14 |
| header, batch 4 (reverse commit) | 14 (2.1 us) | 22.8 |
| header, batch 16 (reverse commit) | 24 (3.0 us) | 35 |
| cursor, batch 16 | >= 30 (1.9 us) | 50 |
| raw header, batch 1 | 22 (9.0 us) | 114 |

What was found on the way (perf counters per message at a fixed rate):
- The publisher was the bottleneck: `claimSingleMessage` did not inline (argument errors with string building,
  padding and waits in one method) - 216 instructions and 38 stores per message. The hot path is now small.
- With batches the readers chased the publisher through the batch: commit words written in order, each reader moved
  to the next message while its line was still being written. Commits now go last message first; a reader that sees
  the first commit sees the whole batch.
- Deferring the header to publish (so it is written next to the commit word) did not help (8-10 vs 10-12 MT/s with
  the header written at claim) and was dropped.
- Batch 1: the commit word sits in the header and the payload starts in the same cache line, so readers waiting
  for the commit hold that line while the payload is written - two ownership transfers per message where raw
  (commit word last in the slot) needs one. Addressed by `FrameLayout.LINE_ALIGNED`, below.

## Line-aligned frames (2026-10-22)

`FrameLayout.LINE_ALIGNED` (`-Dperf.frames=aligned`): the header fills the last 32 bytes of a cache line, the payload
starts on the next line, frames round up to whole lines (80-byte messages take 128); with HEADER_COMMIT the header is
written at publish together with the commit word. Java does not expose array addresses, so the layout assumes the
ring is a G1 humongous array (region-aligned, lines start at element 6); the wrong-phase rows are the control.
`ab_frames.sh`, header-commit, capacity with p99 <= 10 us:

| Scenario | Variant | Capacity, MT/s | p99 at 8 MT/s | Burst, MT/s |
|---|---|---|---|---|
| SHARDED4 | packed, batch 1 | 8 | 1.78 us | 12.2 |
| | aligned, batch 1 | 12 (3.7 us) | 1.51 us | 14.9 |
| | aligned with a wrong line phase, batch 1 | 10 | 1.94 us | 11.5 |
| | aligned, batch 16 | >= 30 (3.4 us) | 1.47 us | 29.7 |
| MULTICAST, 5 readers | packed, batch 1 | 12 | 1.44 us | 16.6 |
| | aligned, batch 1 | 16 (2.7 us) | 0.47 us | 18.7 |
| | aligned with a wrong line phase, batch 1 | 4 | saturated | 8.5 |
| | aligned, batch 16 | >= 30 (1.8 us) | 0.51 us | 34.4 |

Aligned frames lift header-commit at batch 1 by 30-50% and cut p99 at mid load up to 3x (MULTICAST); a wrong line
phase loses all of it, which confirms the cache-line explanation. Raw header-commit (64-byte slots, commit word
last) still does more at batch 1 (SHARDED4 22 MT/s).

## Key-based reordering, E5 (2026-10-22)

`ReorderingFlowProcessor` takes a `ReorderingMessageHandler`: `signature()` gives a 64-bit key set of a message (-1
orders it against everything), `tryHandle()` may refuse a message that is not ready yet. A refused message is
postponed (up to 64), later messages go ahead of it unless their signature intersects a postponed one, and the
released fence stops at the oldest postponed message. `-Dperf.bypass` runs the SHARDED4 join this way: the message
is not ready while its owner shard is behind, the signature is one bit of a 64-bucket key hash, and the handler counts
an error if keys of a bucket arrive out of order (`-Dperf.bypassNoOrder`, empty signatures, gives ~540k errors).
`charts_e5.sh`, cursor publication, p50 / p90 / p99 in ns.

Shard S0 spends ~1 us per message (`perf.slowShard=0`, `perf.slowWork=700`), so it is busy 25/50/75% at 1/2/3 MT/s:

| Rate | revelator, in order | revelator, bypass | disruptor | raw |
|---|---|---|---|---|
| 1 MT/s | 379 / 1335 / 1383 | 409 / 1407 / 1471 | 531 / 1487 / 1583 | 419 / 1383 / 1423 |
| 2 MT/s | 763 / 1335 / 1383 | 397 / 1407 / 1463 | 771 / 1479 / 1575 | 803 / 1375 / 1423 |
| 3 MT/s | 979 / 1343 / 1487 | 393 / 1407 / 1503 | 1031 / 1463 / 1655 | 983 / 1351 / 1399 |

Equal shards, nothing to reorder:

| Rate | revelator, in order | revelator, bypass |
|---|---|---|
| 1 MT/s | 391 / 479 / 547 | 383 / 443 / 531 |
| 5 MT/s | 467 / 619 / 787 | 501 / 727 / 959 |

An in-order join stalls behind each S0 message, so p50 follows the slow shard's queue (~1 us at 75% busy) in all
three implementations; with the bypass the messages of fast shards stay at ~0.4 us at any S0 load and only S0's own
messages are slow (p90 ~1.4 us everywhere), p99.999 ~4 us against 8-10 us. Where nothing can be reordered the
postpone-and-retry path costs +7% p50 and +22% p99 at 5 MT/s; a join that spins on the head message while the
postponed list is empty would remove most of it.
