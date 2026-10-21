package exchange.core2.revelator.perf;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.fences.AggregatingMinFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.processors.simple.SimpleFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;

import java.util.ArrayList;
import java.util.List;

public final class RevelatorBench implements Bench {

    static final byte MSG_TYPE = 1;
    static final int PAYLOAD_LONGS = 6;

    private final List<Stage> stages;
    private final Scenario scenario;
    private final int ringLongs;
    private Revelator revelator;

    public RevelatorBench(Scenario scenario, int ringLongs, int work) {
        this.scenario = scenario;
        this.ringLongs = ringLongs;
        this.stages = Bench.createStages(scenario, work);
    }

    @Override
    public void start(PinnedThreadFactory threadFactory) {
        revelator = Revelator.create(ringLongs, factory(), threadFactory);
        revelator.start();
    }

    private IFlowProcessorsFactory factory() {
        final List<SimpleMessageHandler> h = stages.stream().map(StageHandler::new).map(SimpleMessageHandler.class::cast).toList();
        return switch (scenario) {
            case UNICAST -> ProcessorsFactories.single(h.get(0));
            case PIPELINE3 -> ProcessorsFactories.chain(h);
            case MULTICAST3 -> ProcessorsFactories.parallel(h);
            case DIAMOND -> (inbound, config) -> {
                final SimpleFlowProcessor a = new SimpleFlowProcessor(h.get(0), inbound, config);
                final SimpleFlowProcessor b = new SimpleFlowProcessor(h.get(1), inbound, config);
                final IFence ab = new AggregatingMinFence(new IFence[]{a.getReleasingFence(), b.getReleasingFence()});
                final SimpleFlowProcessor c = new SimpleFlowProcessor(h.get(2), ab, config);
                return new IFlowProcessorsFactory.ProcessorsChain(List.of(a, b, c), c.getReleasingFence());
            };
            case SHARDED4 -> (inbound, config) -> {
                final List<IFlowProcessor> processors = new ArrayList<>();
                final IFence[] shardFences = new IFence[Scenario.SHARDS];
                for (int i = 0; i < Scenario.SHARDS; i++) {
                    final SimpleFlowProcessor shard = new SimpleFlowProcessor(h.get(i), inbound, config);
                    processors.add(shard);
                    shardFences[i] = shard.getReleasingFence();
                }
                // the join reads the producer's fence and then waits only for the shard owning each message
                final SimpleFlowProcessor join = new SimpleFlowProcessor(
                        new ShardJoinHandler(stages.get(Scenario.SHARDS), shardFences), inbound, config);
                processors.add(join);
                final List<IFence> all = new ArrayList<>(List.of(shardFences));
                all.add(join.getReleasingFence());
                return new IFlowProcessorsFactory.ProcessorsChain(processors, new AggregatingMinFence(all));
            };
        };
    }

    @Override
    public void publish(final long timestamp, final long key) {
        final long seq = revelator.claimSingleMessage(PAYLOAD_LONGS, timestamp, key & 0x00FF_FFFF_FFFF_FFFFL, MSG_TYPE);
        revelator.writeLongData(seq, 0, key, key + 1, key + 2, key + 3, key + 4, key + 5);
        revelator.publish(seq + PAYLOAD_LONGS);
    }

    @Override
    public List<Stage> stages() {
        return stages;
    }

    static long payloadSum(long[] buffer, int index) {
        return buffer[index] + buffer[index + 1] + buffer[index + 2] + buffer[index + 3] + buffer[index + 4] + buffer[index + 5];
    }

    private record StageHandler(Stage stage) implements SimpleMessageHandler {
        @Override
        public void handleMessage(long[] buffer, int index, int msgSize, long timestamp, long globalOffset, long correlationId, byte msgType) {
            if (msgType != MSG_TYPE) {
                return;
            }
            final long key = buffer[index];
            if (stage.accepts(key)) {
                stage.process(timestamp, key, payloadSum(buffer, index));
            }
        }
    }

    /**
     * Conditional fence: waits for the shard that owns the message, not for all shards; bounds already seen are cached.
     */
    private static final class ShardJoinHandler implements SimpleMessageHandler {

        private final Stage stage;
        private final IFence[] shardFences;
        private final long[] knownProgress;
        private final long shardMask;

        ShardJoinHandler(Stage stage, IFence[] shardFences) {
            this.stage = stage;
            this.shardFences = shardFences;
            this.knownProgress = new long[shardFences.length];
            this.shardMask = shardFences.length - 1;
        }

        @Override
        public void handleMessage(long[] buffer, int index, int msgSize, long timestamp, long globalOffset, long correlationId, byte msgType) {
            if (msgType != MSG_TYPE) {
                return;
            }
            final long key = buffer[index];
            final int shard = (int) (key & shardMask);
            final long messageEnd = globalOffset + Revelator.MSG_HEADER_SIZE + msgSize;
            if (knownProgress[shard] < messageEnd) {
                final IFence fence = shardFences[shard];
                long progress;
                while ((progress = fence.getAcquire(0L)) < messageEnd) {
                    Thread.onSpinWait();
                }
                knownProgress[shard] = progress;
            }
            stage.process(timestamp, key, payloadSum(buffer, index));
        }
    }
}
