package exchange.core2.revelator.perf;

import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventProcessorFactory;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;
import java.util.stream.IntStream;

/**
 * LMAX Disruptor 4 in its fastest configuration for this job: single producer, busy-spin wait strategy,
 * preallocated events with primitive fields, the same pinned threads as Revelator.
 */
public final class DisruptorBench implements Bench {

    private final List<Stage> stages;
    private final Scenario scenario;
    private final int ringSize;
    private final int maxBatch;
    private final boolean results;
    private RingBuffer<Event> ringBuffer;

    public DisruptorBench(Scenario scenario, int ringSize, int work, int maxBatch, boolean results) {
        this.scenario = scenario;
        this.ringSize = ringSize;
        this.maxBatch = maxBatch;
        this.results = results && scenario != Scenario.PIPELINE3_1T;
        this.stages = Bench.createStages(scenario, work);
    }

    @Override
    public void start(PinnedThreadFactory threadFactory) {
        // results go into the event itself, the usual Disruptor way; plain runs keep the smaller event
        final Disruptor<Event> disruptor = new Disruptor<>(
                results ? ResultEvent::new : Event::new, ringSize, threadFactory, ProducerType.SINGLE, new BusySpinWaitStrategy());

        if (scenario == Scenario.PIPELINE3_1T) {
            disruptor.handleEventsWith(processor(new FusedHandler(stages.get(0), stages.get(1), stages.get(2))));
        } else {
            final List<EventProcessorFactory<Event>> f = IntStream.range(0, stages.size())
                    .mapToObj(i -> processor(handler(i))).toList();
            switch (scenario) {
                case UNICAST -> disruptor.handleEventsWith(f.get(0));
                case PIPELINE3 -> disruptor.handleEventsWith(f.get(0)).then(f.get(1)).then(f.get(2));
                case MULTICAST3 -> disruptor.handleEventsWith(f.get(0), f.get(1), f.get(2));
                case DIAMOND -> disruptor.handleEventsWith(f.get(0), f.get(1)).then(f.get(2));
                // the join can only wait for the minimum over all shards
                case SHARDED4 -> disruptor.handleEventsWith(f.get(0), f.get(1), f.get(2), f.get(3)).then(f.get(4));
                default -> throw new IllegalStateException();
            }
        }
        ringBuffer = disruptor.start();
    }

    private StageHandler handler(int stage) {
        final boolean join = scenario == Scenario.SHARDED4 && stage == Scenario.SHARDS;
        return new StageHandler(stages.get(stage), results, results ? Bench.upstreamResults(scenario, stage) : new int[0], results && join);
    }

    private EventProcessorFactory<Event> processor(EventHandler<Event> handler) {
        return (rb, barrierSequences) -> new BatchEventProcessorBuilder()
                .setMaxBatchSize(maxBatch)
                .build(rb, rb.newBarrier(barrierSequences), handler);
    }

    @Override
    public void publish(final long timestamp, final long key) {
        final long seq = ringBuffer.next();
        final Event e = ringBuffer.get(seq);
        e.timestamp = timestamp;
        e.key = key;
        e.v1 = key + 1;
        e.v2 = key + 2;
        e.v3 = key + 3;
        e.v4 = key + 4;
        e.v5 = key + 5;
        ringBuffer.publish(seq);
    }

    @Override
    public List<Stage> stages() {
        return stages;
    }

    static class Event {
        long timestamp;
        long key;
        long v1, v2, v3, v4, v5;

        long sum() {
            return key + v1 + v2 + v3 + v4 + v5;
        }
    }

    static final class ResultEvent extends Event {
        long r0, r1, r2, r3, r4;

        long result(int stage) {
            return switch (stage) {
                case 0 -> r0;
                case 1 -> r1;
                case 2 -> r2;
                case 3 -> r3;
                default -> r4;
            };
        }

        void result(int stage, long value) {
            switch (stage) {
                case 0 -> r0 = value;
                case 1 -> r1 = value;
                case 2 -> r2 = value;
                case 3 -> r3 = value;
                default -> r4 = value;
            }
        }
    }

    private record StageHandler(Stage stage, boolean results, int[] upIds, boolean shardJoin) implements EventHandler<Event> {
        @Override
        public void onEvent(Event e, long sequence, boolean endOfBatch) {
            final long key = e.key;
            if (stage.accepts(key)) {
                final long sum = e.sum();
                if (results) {
                    final ResultEvent re = (ResultEvent) e;
                    for (int upId : upIds) {
                        stage.verify(upId, sum, re.result(upId));
                    }
                    if (shardJoin) {
                        final int shard = (int) (key & (Scenario.SHARDS - 1));
                        stage.verify(shard, sum, re.result(shard));
                    }
                    re.result(stage.id(), stage.process(e.timestamp, key, sum));
                } else {
                    stage.process(e.timestamp, key, sum);
                }
            }
        }
    }

    /**
     * PIPELINE3_1T: one handler runs A, B and C for each event.
     */
    private record FusedHandler(Stage a, Stage b, Stage c) implements EventHandler<Event> {
        @Override
        public void onEvent(Event e, long sequence, boolean endOfBatch) {
            final long sum = e.sum();
            a.process(e.timestamp, e.key, sum);
            b.process(e.timestamp, e.key, sum);
            c.process(e.timestamp, e.key, sum);
        }
    }
}
