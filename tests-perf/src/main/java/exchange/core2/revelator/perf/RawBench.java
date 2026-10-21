package exchange.core2.revelator.perf;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written floor: a long[] ring of 64-byte slots, one progress counter per stage, no framework code. Waits
 * re-read only the dependencies whose cached bound is behind; the SHARDED4 join waits for the owner shard only.
 */
public final class RawBench implements Bench {

    private static final int SLOT = 8;        // longs per message: timestamp, 6 payload longs, 1 spare
    private static final int MAX_BATCH = 512; // a stage publishes its progress at least every 512 messages

    private final Scenario scenario;
    private final List<Stage> stages;
    private final boolean results;
    private final int ringMsgs;
    private final long mask;
    private final long[] ring;
    private final long[][] columns;           // stage results indexed by sequence (-Dperf.results mode)
    private final Cursor published = new Cursor();
    private final Cursor[] progress;
    private Cursor[] gating;                  // stages the publisher must not lap

    private long nextSeq = 0;
    private long gateKnown = 0;

    public RawBench(Scenario scenario, int ringMsgs, int work, boolean results) {
        this.scenario = scenario;
        this.stages = Bench.createStages(scenario, work);
        this.results = results && scenario != Scenario.PIPELINE3_1T;
        this.ringMsgs = ringMsgs;
        this.mask = ringMsgs - 1;
        this.ring = new long[ringMsgs * SLOT];
        this.columns = new long[stages.size()][];
        if (this.results) {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new long[ringMsgs];
            }
        }
        this.progress = new Cursor[stages.size()];
        for (int i = 0; i < progress.length; i++) {
            progress[i] = new Cursor();
        }
    }

    @Override
    public void start(PinnedThreadFactory threadFactory) {
        final List<Runnable> runners = new ArrayList<>();
        switch (scenario) {
            case UNICAST -> {
                runners.add(runner(0, published));
                gating = new Cursor[]{progress[0]};
            }
            case PIPELINE3 -> {
                runners.add(runner(0, published));
                runners.add(runner(1, progress[0]));
                runners.add(runner(2, progress[1]));
                gating = new Cursor[]{progress[2]};
            }
            case PIPELINE3_1T -> {
                runners.add(new FusedRunner());
                gating = new Cursor[]{progress[2]};
            }
            case MULTICAST3 -> {
                runners.add(runner(0, published));
                runners.add(runner(1, published));
                runners.add(runner(2, published));
                gating = new Cursor[]{progress[0], progress[1], progress[2]};
            }
            case DIAMOND -> {
                runners.add(runner(0, published));
                runners.add(runner(1, published));
                runners.add(runner(2, progress[0], progress[1]));
                gating = new Cursor[]{progress[2]};
            }
            case SHARDED4 -> {
                for (int i = 0; i < Scenario.SHARDS; i++) {
                    runners.add(runner(i, published));
                }
                runners.add(new JoinRunner());
                gating = progress.clone();
            }
        }
        runners.forEach(r -> threadFactory.newThread(r).start());
    }

    private StageRunner runner(int stage, Cursor... deps) {
        final int[] upIds = results ? Bench.upstreamResults(scenario, stage) : new int[0];
        final long[][] upColumns = new long[upIds.length][];
        for (int i = 0; i < upIds.length; i++) {
            upColumns[i] = columns[upIds[i]];
        }
        return new StageRunner(stages.get(stage), progress[stage], deps, columns[stage], upIds, upColumns);
    }

    @Override
    public void publish(final long timestamp, final long key) {
        final long seq = nextSeq;
        if (seq - ringMsgs >= gateKnown) {
            gateKnown = awaitGate(seq - ringMsgs);
        }
        final int i = (int) (seq & mask) * SLOT;
        ring[i] = timestamp;
        ring[i + 1] = key;
        ring[i + 2] = key + 1;
        ring[i + 3] = key + 2;
        ring[i + 4] = key + 3;
        ring[i + 5] = key + 4;
        ring[i + 6] = key + 5;
        nextSeq = seq + 1;
        published.set(seq + 1);
    }

    private long awaitGate(long wrapSeq) {
        long min;
        do {
            min = Long.MAX_VALUE;
            for (Cursor c : gating) {
                min = Math.min(min, c.get());
            }
            if (min <= wrapSeq) {
                Thread.onSpinWait();
            }
        } while (min <= wrapSeq);
        return min;
    }

    @Override
    public List<Stage> stages() {
        return stages;
    }

    private long payloadSum(int i) {
        return ring[i + 1] + ring[i + 2] + ring[i + 3] + ring[i + 4] + ring[i + 5] + ring[i + 6];
    }

    /**
     * @return the smallest bound of the dependencies, greater than pos; re-reads only the ones known to be behind
     */
    private static long await(long pos, Cursor[] deps, long[] known) {
        long min = Long.MAX_VALUE;
        for (int d = 0; d < deps.length; d++) {
            long v = known[d];
            if (v <= pos) {
                while ((v = deps[d].get()) <= pos) {
                    Thread.onSpinWait();
                }
                known[d] = v;
            }
            min = Math.min(min, v);
        }
        return min;
    }

    private final class StageRunner implements Runnable {
        private final Stage stage;
        private final Cursor own;
        private final Cursor[] deps;
        private final long[] column;
        private final int[] upIds;
        private final long[][] upColumns;

        StageRunner(Stage stage, Cursor own, Cursor[] deps, long[] column, int[] upIds, long[][] upColumns) {
            this.stage = stage;
            this.own = own;
            this.deps = deps;
            this.column = column;
            this.upIds = upIds;
            this.upColumns = upColumns;
        }

        @Override
        public void run() {
            final long[] known = new long[deps.length];
            long pos = 0;
            long available = 0;
            while (true) {
                if (pos >= available) {
                    available = await(pos, deps, known);
                }
                final long end = Math.min(available, pos + MAX_BATCH);
                for (; pos < end; pos++) {
                    final int i = (int) (pos & mask) * SLOT;
                    final long key = ring[i + 1];
                    if (stage.accepts(key)) {
                        final long sum = payloadSum(i);
                        final int c = (int) (pos & mask);
                        for (int u = 0; u < upIds.length; u++) {
                            stage.verify(upIds[u], sum, upColumns[u][c]);
                        }
                        final long r = stage.process(ring[i], key, sum);
                        if (column != null) {
                            column[c] = r;
                        }
                    }
                }
                own.set(pos);
            }
        }
    }

    /**
     * SHARDED4 join: follows the publisher and waits only for the shard that owns each message.
     */
    private final class JoinRunner implements Runnable {
        @Override
        public void run() {
            final Stage stage = stages.get(Scenario.SHARDS);
            final Cursor own = progress[Scenario.SHARDS];
            final Cursor[] deps = {published};
            final long[] known = new long[1];
            final long[] shardKnown = new long[Scenario.SHARDS];
            long pos = 0;
            long available = 0;
            while (true) {
                if (pos >= available) {
                    available = await(pos, deps, known);
                }
                final long end = Math.min(available, pos + MAX_BATCH);
                for (; pos < end; pos++) {
                    final int i = (int) (pos & mask) * SLOT;
                    final long key = ring[i + 1];
                    final int shard = (int) (key & (Scenario.SHARDS - 1));
                    if (shardKnown[shard] <= pos) {
                        long v;
                        while ((v = progress[shard].get()) <= pos) {
                            Thread.onSpinWait();
                        }
                        shardKnown[shard] = v;
                    }
                    final long sum = payloadSum(i);
                    if (results) {
                        stage.verify(shard, sum, columns[shard][(int) (pos & mask)]);
                    }
                    stage.process(ring[i], key, sum);
                }
                own.set(pos);
            }
        }
    }

    /**
     * PIPELINE3_1T: one thread runs A, B and C for each message.
     */
    private final class FusedRunner implements Runnable {
        @Override
        public void run() {
            final Stage a = stages.get(0), b = stages.get(1), c = stages.get(2);
            final Cursor own = progress[2];
            final Cursor[] deps = {published};
            final long[] known = new long[1];
            long pos = 0;
            long available = 0;
            while (true) {
                if (pos >= available) {
                    available = await(pos, deps, known);
                }
                final long end = Math.min(available, pos + MAX_BATCH);
                for (; pos < end; pos++) {
                    final int i = (int) (pos & mask) * SLOT;
                    final long key = ring[i + 1];
                    final long sum = payloadSum(i);
                    a.process(ring[i], key, sum);
                    b.process(ring[i], key, sum);
                    c.process(ring[i], key, sum);
                }
                own.set(pos);
            }
        }
    }

    // progress counter padded to 128 bytes on both sides
    abstract static class CursorLhsPad {
        long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15, p16;
    }

    abstract static class CursorValue extends CursorLhsPad {
        long value;
    }

    static final class Cursor extends CursorValue {
        long p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32;

        private static final VarHandle VALUE;

        static {
            try {
                VALUE = MethodHandles.lookup().findVarHandle(CursorValue.class, "value", long.class);
            } catch (ReflectiveOperationException ex) {
                throw new ExceptionInInitializerError(ex);
            }
        }

        long get() {
            return (long) VALUE.getAcquire(this);
        }

        void set(long v) {
            VALUE.setRelease(this, v);
        }
    }
}
