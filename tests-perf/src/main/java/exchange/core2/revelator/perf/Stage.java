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
}

abstract class StageRhsPad extends StageFields {
    long p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32;
}

/**
 * Work done by one stage of a scenario - the same code runs inside a Revelator handler and a Disruptor handler,
 * so the frameworks differ only in how a message reaches it.
 */
public final class Stage extends StageRhsPad {

    // every 16th message (by key) records its end-to-end latency
    static final long SAMPLE_MASK = 15;

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

    Stage(String name, int id, int shard, int shards, int work, boolean terminal) {
        this.name = name;
        this.id = id;
        this.shard = shard;
        this.shardMask = shards - 1;
        this.work = work;
        this.recorder = terminal ? new SingleWriterRecorder(60_000_000_000L, 2) : null;
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
            long x = payloadSum;
            for (int i = 0; i < work; i++) {
                x = x * 6364136223846793005L + 1442695040888963407L;
            }
            sink ^= x;
        }
        if (recorder != null && (key & SAMPLE_MASK) == 0) {
            recorder.recordValue(Math.max(0L, System.nanoTime() - timestamp));
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

    String name() {
        return name;
    }

    static long payloadSum(long key) {
        return 6 * key + 15; // key, key+1 .. key+5
    }
}
