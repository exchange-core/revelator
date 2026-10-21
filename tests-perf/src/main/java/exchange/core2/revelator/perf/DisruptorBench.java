package exchange.core2.revelator.perf;

import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventProcessorFactory;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;

/**
 * LMAX Disruptor 4 in its fastest configuration for this job: single producer, busy-spin wait strategy,
 * preallocated events with primitive fields, the same pinned threads as Revelator.
 */
public final class DisruptorBench implements Bench {

    private final List<Stage> stages;
    private final Scenario scenario;
    private final int ringSize;
    private final int maxBatch;
    private RingBuffer<Event> ringBuffer;

    public DisruptorBench(Scenario scenario, int ringSize, int work, int maxBatch) {
        this.scenario = scenario;
        this.ringSize = ringSize;
        this.maxBatch = maxBatch;
        this.stages = Bench.createStages(scenario, work);
    }

    @Override
    public void start(PinnedThreadFactory threadFactory) {
        final Disruptor<Event> disruptor = new Disruptor<>(
                Event::new, ringSize, threadFactory, ProducerType.SINGLE, new BusySpinWaitStrategy());

        final List<EventProcessorFactory<Event>> f = stages.stream().map(this::processor).toList();
        switch (scenario) {
            case UNICAST -> disruptor.handleEventsWith(f.get(0));
            case PIPELINE3 -> disruptor.handleEventsWith(f.get(0)).then(f.get(1)).then(f.get(2));
            case MULTICAST3 -> disruptor.handleEventsWith(f.get(0), f.get(1), f.get(2));
            case DIAMOND -> disruptor.handleEventsWith(f.get(0), f.get(1)).then(f.get(2));
            // the join can only wait for the minimum over all shards
            case SHARDED4 -> disruptor.handleEventsWith(f.get(0), f.get(1), f.get(2), f.get(3)).then(f.get(4));
        }
        ringBuffer = disruptor.start();
    }

    private EventProcessorFactory<Event> processor(Stage stage) {
        final StageHandler handler = new StageHandler(stage);
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

    static final class Event {
        long timestamp;
        long key;
        long v1, v2, v3, v4, v5;
    }

    private record StageHandler(Stage stage) implements EventHandler<Event> {
        @Override
        public void onEvent(Event e, long sequence, boolean endOfBatch) {
            final long key = e.key;
            if (stage.accepts(key)) {
                stage.process(e.timestamp, key, key + e.v1 + e.v2 + e.v3 + e.v4 + e.v5);
            }
        }
    }
}
