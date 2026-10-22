package exchange.core2.revelator.perf;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

// 128 bytes of padding around the fields written by the stage thread
abstract class StageLhsPad {
    long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15, p16;
}

abstract class StageFields extends StageLhsPad {
    long checksum;
    long sink;
    long errors;    // upstream results that did not match
    long processed; // read by the producer thread to detect completion
    int sampleCount;
}

abstract class StageRhsPad extends StageFields {
    long p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32;
}

/**
 * Work done by one stage of a scenario - the same code runs inside a Revelator handler and a Disruptor handler,
 * so the frameworks differ only in how a message reaches it.
 */
public final class Stage extends StageRhsPad {

    // one message in 16 records its end-to-end latency, chosen by a key hash: key % 16 == 0 would only ever pick
    // messages of SHARDED4 shard 0
    static boolean sampled(long key) {
        return (key * 0x9E37_79B9_7F4A_7C15L) >>> 60 == 0;
    }

    private static final VarHandle PROCESSED;

    static {
        try {
            PROCESSED = MethodHandles.lookup().findVarHandle(StageFields.class, "processed", long.class);
        } catch (ReflectiveOperationException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private final String name;
    private final int id;         // index in the scenario's stage list
    private final long shard;     // -1: every message, otherwise only keys with (key & shardMask) == shard
    private final long shardMask;
    private final int work;       // extra dependent multiply-adds per message
    private final SingleWriterRecorder recorder; // null unless the stage ends the scenario
    private final long[] samples;                 // raw latency samples for charts (-Dperf.samples), or null

    Stage(String name, int id, int shard, int shards, int work, boolean terminal) {
        this.name = name;
        this.id = id;
        this.shard = shard;
        this.shardMask = shards - 1;
        this.work = work;
        this.recorder = terminal ? new SingleWriterRecorder(60_000_000_000L, 2) : null;
        this.samples = terminal && System.getProperty("perf.samples") != null
                ? new long[Integer.getInteger("perf.samplesMax", 1_000_000)]
                : null;
    }

    boolean accepts(final long key) {
        return shard < 0 || (key & shardMask) == shard;
    }

    /**
     * @return the stage result for this message (written somewhere only in -Dperf.results mode)
     */
    long process(final long timestamp, final long key, final long payloadSum) {
        checksum += payloadSum;
        if (work > 0) {
            // xorshift rounds: a dependent chain the JIT cannot fold (an LCG loop of 1000 steps cost ~30 ns)
            long x = payloadSum | 1;
            for (int i = 0; i < work; i++) {
                x ^= x << 13;
                x ^= x >>> 7;
                x ^= x << 17;
            }
            sink ^= x;
        }
        if (recorder != null && sampled(key)) {
            final long latency = Math.max(0L, System.nanoTime() - timestamp);
            recorder.recordValue(latency);
            if (samples != null && sampleCount < samples.length) {
                samples[sampleCount++] = latency;
            }
        }
        PROCESSED.setRelease(this, processed + 1);
        return resultOf(id, payloadSum);
    }

    /**
     * Checks a result written by an upstream stage for the same message.
     */
    void verify(final int upstreamId, final long payloadSum, final long value) {
        if (value != resultOf(upstreamId, payloadSum)) {
            errors++;
        }
    }

    static long resultOf(int stageId, long payloadSum) {
        return payloadSum * (stageId + 2);
    }

    long errors() {
        return errors;
    }

    int id() {
        return id;
    }

    long processed() {
        return (long) PROCESSED.getAcquire(this);
    }

    long checksum() {
        return checksum;
    }

    boolean isTerminal() {
        return recorder != null;
    }

    Histogram intervalHistogram() {
        return recorder.getIntervalHistogram();
    }

    /**
     * Samples recorded since the last call (call only while the stage is idle; the next publication makes the reset
     * visible to the stage thread).
     */
    long[] takeSamples() {
        if (samples == null) {
            return new long[0];
        }
        final long[] copy = java.util.Arrays.copyOf(samples, sampleCount);
        sampleCount = 0;
        return copy;
    }

    String name() {
        return name;
    }

    static long payloadSum(long key) {
        return 6 * key + 15; // key, key+1 .. key+5
    }
}
