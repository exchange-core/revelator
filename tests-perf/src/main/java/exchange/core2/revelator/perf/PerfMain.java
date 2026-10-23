package exchange.core2.revelator.perf;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.revelator.FrameLayout;
import exchange.core2.revelator.Revelator;
import org.HdrHistogram.Histogram;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

/**
 * Revelator vs Disruptor on the same scenario, one framework per JVM. Settings (system properties):
 * <pre>
 * perf.impl      revelator | disruptor | raw (hand-written ring, the floor)
 * perf.scenario  UNICAST | PIPELINE3 | PIPELINE3_1T | MULTICAST3 | DIAMOND | SHARDED4 | MULTICAST (perf.readers)
 * perf.stopAtP99Us  stop the latency sweep after the first step with p99 above this or a publisher that fell behind
 * perf.rates     offered loads for the latency phase, millions of messages per second (default 1,5,10,20)
 * perf.seconds   duration of each latency step (default 2)
 * perf.burst     messages per throughput burst (default 20M), perf.bursts - number of bursts (default 5)
 * perf.work      extra xorshift rounds per message in every stage (default 0 - pure transport; ~1.4 ns each on rhel-lat)
 * perf.results   stages write a result that downstream stages read and verify (default false): Disruptor into the
 *                event, Revelator and raw into a separate column per stage
 * perf.ringMsgs  Disruptor ring size; Revelator gets ringMsgs*8 longs, about the same bytes (default 131072)
 * perf.maxBatch  Disruptor maxBatchSize (default unlimited, as in the DSL)
 * perf.batch     publisher makes messages visible every N messages (default 1) and always before it waits for
 *                the schedule, so batching never delays a message the publisher could not have followed anyway
 * perf.publication  cursor | header (commit word per message; Revelator and raw only)
 * perf.frames    packed | aligned (Revelator: headers at the end of a cache line, payload on the next one;
 *                -Drevelator.linePhase sets where lines start in the ring)
 * perf.cpus      publisher CPU, then consumer CPUs (default: reserved/isolated CPUs, highest first)
 * perf.out       CSV file to append results to, perf.label - run label for it
 * perf.samples   directory for raw latency samples of each latency step: &lt;dir&gt;/&lt;scenario&gt;-&lt;rate&gt;MTs/&lt;perf.series&gt;.csv,
 *                column latency_ps (exchange-charts format); perf.series defaults to perf.impl
 * perf.slowShard, perf.slowWork  SHARDED4: shard i does slowWork multiply-adds per message
 * </pre>
 * Latency is open-loop: message j is due at t0 + j/rate and carries that due time, so a publisher falling behind
 * shows up as latency instead of being hidden (no coordinated omission).
 */
public final class PerfMain {

    private static final int CHUNK = 4096;

    private final Bench bench;
    private final List<Stage> terminals;
    private final int batch;

    private long nextKey = 0;      // messages published so far
    private long expectedSum = 0;  // checksum every terminal stage must reach

    private PerfMain(Bench bench, int batch) {
        this.bench = bench;
        this.batch = batch;
        this.terminals = bench.stages().stream().filter(Stage::isTerminal).toList();
    }

    public static void main(String[] args) throws IOException {

        final String impl = System.getProperty("perf.impl", "revelator");
        final Scenario scenario = Scenario.valueOf(System.getProperty("perf.scenario", "UNICAST"));
        final double[] rates = Arrays.stream(System.getProperty("perf.rates", "1,5,10,20").split(","))
                .mapToDouble(Double::parseDouble).toArray();
        final double seconds = Double.parseDouble(System.getProperty("perf.seconds", "2"));
        final long burst = Long.getLong("perf.burst", 20_000_000L);
        final int bursts = Integer.getInteger("perf.bursts", 5);
        final int work = Integer.getInteger("perf.work", 0);
        final int ringMsgs = Integer.getInteger("perf.ringMsgs", 131072);
        final int maxBatch = Integer.getInteger("perf.maxBatch", Integer.MAX_VALUE);
        final boolean results = Boolean.getBoolean("perf.results");
        final int batch = Integer.getInteger("perf.batch", 1);
        final boolean headerCommit = switch (System.getProperty("perf.publication", "cursor")) {
            case "cursor" -> false;
            case "header" -> true;
            default -> throw new IllegalArgumentException("perf.publication: cursor | header");
        };
        if (headerCommit && impl.equals("disruptor")) {
            throw new IllegalArgumentException("Disruptor has no header-commit publication");
        }
        final int[] cpus = cpus(scenario.threads() + 1);

        final Bench bench = switch (impl) {
            case "revelator" -> new RevelatorBench(scenario, ringMsgs * 8, work, results,
                    headerCommit ? Revelator.PublicationMode.HEADER_COMMIT : Revelator.PublicationMode.CURSOR,
                    "aligned".equals(System.getProperty("perf.frames")) ? FrameLayout.lineAligned() : FrameLayout.packed());
            case "disruptor" -> new DisruptorBench(scenario, ringMsgs, work, maxBatch, results);
            case "raw" -> new RawBench(scenario, ringMsgs, work, results, headerCommit);
            default -> throw new IllegalArgumentException("perf.impl: " + impl);
        };

        System.out.printf("%s %s publisher cpu %d, consumers %s, work=%d, results=%s, batch=%d, publication=%s, frames=%s, ringMsgs=%d%n",
                impl, scenario, cpus[0], Arrays.toString(Arrays.copyOfRange(cpus, 1, cpus.length)), work, results,
                batch, headerCommit ? "header" : "cursor", System.getProperty("perf.frames", "packed"), ringMsgs);

        // consumer threads are created before the publisher pins itself, so they never inherit its single CPU
        bench.start(new PinnedThreadFactory(Arrays.copyOfRange(cpus, 1, cpus.length)));

        final List<String> csv = new ArrayList<>();
        final String prefix = System.getProperty("perf.label", "") + "," + impl + "," + scenario + ",";

        try (CpuAffinity ignore = CpuAffinity.acquireCore(cpus[0])) {
            final PerfMain perf = new PerfMain(bench, batch);

            for (int i = 0; i < 3; i++) {
                perf.latencyStep(Math.min(1.0, rates[0]), 0.5);
                perf.burst(Math.min(burst, 2_000_000L));
            }

            final long gc0 = gcCount();
            final long jit0 = jitMillis();

            final String samplesDir = System.getProperty("perf.samples");
            final String series = System.getProperty("perf.series", impl);
            final double stopAtP99Us = Double.parseDouble(System.getProperty("perf.stopAtP99Us", "0"));
            for (double rate : rates) {
                final String r = perf.latencyStep(rate, seconds);
                System.out.println("latency " + r);
                csv.add(prefix + "latency," + r);
                final String[] f = r.split(",");
                final boolean saturated = Double.parseDouble(f[1]) < rate * 0.97 || Long.parseLong(f[4]) > stopAtP99Us * 1000;
                if (samplesDir != null) {
                    perf.dumpSamples(Path.of(samplesDir, scenario + "-" + (rate == Math.rint(rate) ? String.valueOf((long) rate) : String.valueOf(rate)) + "MTs", series + ".csv"));
                }
                if (stopAtP99Us > 0 && saturated) {
                    System.out.println("stopping the sweep: saturated at " + rate + " MT/s");
                    break;
                }
            }

            final double[] mts = new double[bursts];
            for (int i = 0; i < bursts; i++) {
                mts[i] = perf.burst(burst);
            }
            Arrays.sort(mts);
            final String b = String.format(Locale.ROOT, "best=%.2f median=%.2f MT/s", mts[bursts - 1], mts[bursts / 2]);
            System.out.println("burst " + b);
            csv.add(prefix + String.format(Locale.ROOT, "burst,%.2f,%.2f", mts[bursts - 1], mts[bursts / 2]));

            System.out.printf("measurement window: %d GCs, %d ms of JIT compilation%n", gcCount() - gc0, jitMillis() - jit0);
            perf.verifyChecksums();
        }

        final String out = System.getProperty("perf.out");
        if (out != null) {
            try (FileWriter w = new FileWriter(out, true)) {
                for (String line : csv) {
                    w.write(line + "\n");
                }
            }
        }
        System.exit(0);
    }

    /**
     * @return "target,achieved,p50,p90,p99,p99.9,p99.99,max" (MT/s, ns)
     */
    private String latencyStep(double rateMts, double seconds) {
        drainHistograms();
        final long n = (long) (rateMts * 1_000_000 * seconds);
        final long periodFp = (long) (1024.0 * 1000.0 / rateMts); // ns << 10 between messages
        final long t0 = System.nanoTime() + 100_000;
        for (long j = 0; j < n; j += CHUNK) {
            sendPaced(t0, periodFp, j, Math.min(n, j + CHUNK));
        }
        final long sent = System.nanoTime();
        awaitTerminals();
        final Histogram h = mergedHistogram();
        final double achieved = n * 1000.0 / Math.max(1, sent - t0);
        return String.format(Locale.ROOT, "%.1f,%.2f,%d,%d,%d,%d,%d,%d", rateMts, achieved,
                h.getValueAtPercentile(50), h.getValueAtPercentile(90), h.getValueAtPercentile(99),
                h.getValueAtPercentile(99.9), h.getValueAtPercentile(99.99), h.getMaxValue());
    }

    private void sendPaced(final long t0, final long periodFp, final long from, final long to) {
        final Bench b = bench;
        final int batch = this.batch;
        long key = nextKey;
        long sum = expectedSum;
        long now = System.nanoTime();
        int pending = 0;
        for (long j = from; j < to; j++) {
            final long due = t0 + ((j * periodFp) >> 10);
            if (now < due) {
                // ahead of schedule: publish what is written before waiting
                if (pending != 0) {
                    b.flush();
                    pending = 0;
                }
                do {
                    now = System.nanoTime();
                } while (now < due);
            }
            b.write(due, key);
            if (++pending == batch) {
                b.flush();
                pending = 0;
            }
            sum += Stage.payloadSum(key);
            key++;
        }
        if (pending != 0) {
            b.flush();
        }
        nextKey = key;
        expectedSum = sum;
    }

    /**
     * @return MT/s from the first publish until every terminal stage has processed the last message
     */
    private double burst(long n) {
        drainHistograms();
        final long t0 = System.nanoTime();
        for (long j = 0; j < n; j += CHUNK) {
            sendBurst(Math.min(CHUNK, n - j));
        }
        awaitTerminals();
        return n * 1000.0 / (System.nanoTime() - t0);
    }

    private void sendBurst(final long count) {
        final Bench b = bench;
        final long timestamp = System.nanoTime();
        final int batch = this.batch;
        long key = nextKey;
        long sum = expectedSum;
        int pending = 0;
        for (long j = 0; j < count; j++) {
            b.write(timestamp, key);
            if (++pending == batch) {
                b.flush();
                pending = 0;
            }
            sum += Stage.payloadSum(key);
            key++;
        }
        if (pending != 0) {
            b.flush();
        }
        nextKey = key;
        expectedSum = sum;
    }

    private void dumpSamples(Path file) throws IOException {
        Files.createDirectories(file.getParent());
        final StringBuilder sb = new StringBuilder("latency_ps\n");
        int n = 0;
        for (Stage s : terminals) {
            for (long ns : s.takeSamples()) {
                sb.append(ns * 1000).append('\n');
                n++;
            }
        }
        Files.writeString(file, sb);
        System.out.println("samples " + n + " -> " + file);
    }

    private void awaitTerminals() {
        final long deadline = System.nanoTime() + 60_000_000_000L;
        for (Stage s : terminals) {
            while (s.processed() < nextKey) {
                Thread.onSpinWait();
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(s.name() + " stuck at " + s.processed() + " of " + nextKey);
                }
            }
        }
    }

    private void drainHistograms() {
        terminals.forEach(Stage::intervalHistogram);
        terminals.forEach(Stage::takeSamples);
    }

    private Histogram mergedHistogram() {
        final Histogram h = new Histogram(60_000_000_000L, 2);
        terminals.forEach(s -> h.add(s.intervalHistogram()));
        return h;
    }

    private void verifyChecksums() {
        for (Stage s : terminals) {
            if (s.checksum() != expectedSum) {
                throw new IllegalStateException("checksum mismatch in " + s.name());
            }
        }
        for (Stage s : bench.stages()) {
            if (s.errors() != 0) {
                throw new IllegalStateException(s.errors() + " wrong upstream results in " + s.name());
            }
        }
        System.out.println("checksums OK, messages: " + nextKey);
    }

    private static int[] cpus(int needed) {
        final String prop = System.getProperty("perf.cpus");
        final int[] cpus;
        if (prop != null) {
            cpus = Arrays.stream(prop.split(",")).mapToInt(Integer::parseInt).toArray();
        } else {
            final BitSet reserved = CpuAffinity.reservedCpus();
            cpus = new int[reserved.cardinality()];
            int i = 0;
            for (int c = reserved.length() - 1; c >= 0; c = reserved.previousSetBit(c - 1)) {
                cpus[i++] = c;
            }
        }
        if (cpus.length < needed) {
            throw new IllegalStateException("need " + needed + " CPUs (perf.cpus or isolated), have " + Arrays.toString(cpus));
        }
        return Arrays.copyOf(cpus, needed);
    }

    private static long gcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
    }

    private static long jitMillis() {
        final CompilationMXBean jit = ManagementFactory.getCompilationMXBean();
        return jit != null && jit.isCompilationTimeMonitoringSupported() ? jit.getTotalCompilationTime() : -1;
    }
}
