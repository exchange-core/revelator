package exchange.core2.revelator.perf;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.fences.AggregatingMinFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.processors.pipelined.PipelinedFlowProcessor;
import exchange.core2.revelator.processors.pipelined.PipelinedFlowSession;
import exchange.core2.revelator.processors.pipelined.PipelinedStageHandler;
import exchange.core2.revelator.processors.simple.SimpleFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public final class RevelatorBench implements Bench {

    static final byte MSG_TYPE = 1;
    static final int PAYLOAD_LONGS = 6;

    private final List<Stage> stages;
    private final Scenario scenario;
    private final int ringLongs;
    private final boolean results;
    private final long[][] columns; // stage results indexed by message position, one writer each (-Dperf.results)
    private Revelator revelator;

    public RevelatorBench(Scenario scenario, int ringLongs, int work, boolean results) {
        this.scenario = scenario;
        this.ringLongs = ringLongs;
        this.stages = Bench.createStages(scenario, work);
        this.results = results && scenario != Scenario.PIPELINE3_1T;
        this.columns = new long[stages.size()][];
        if (this.results) {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new long[ringLongs];
            }
        }
    }

    @Override
    public void start(PinnedThreadFactory threadFactory) {
        revelator = Revelator.create(ringLongs, factory(), threadFactory);
        revelator.start();
    }

    private IFlowProcessorsFactory factory() {
        final List<SimpleMessageHandler> h = IntStream.range(0, stages.size()).mapToObj(this::handler).toList();
        return switch (scenario) {
            case UNICAST -> ProcessorsFactories.single(h.get(0));
            case PIPELINE3 -> ProcessorsFactories.chain(h);
            case MULTICAST3 -> ProcessorsFactories.parallel(h);
            case PIPELINE3_1T -> (inbound, config) -> {
                final List<PipelinedStageHandler<PipelinedFlowSession>> pipelined = stages.stream()
                        .map(s -> (PipelinedStageHandler<PipelinedFlowSession>) new PipelinedStage(s, config.getBuffer()))
                        .toList();
                final PipelinedFlowProcessor<PipelinedFlowSession> p = new PipelinedFlowProcessor<>(
                        pipelined, PipelinedFlowSession::new, inbound, config.getIndexMask(), config.getBuffer());
                return new IFlowProcessorsFactory.ProcessorsChain(List.of(p), p.getReleasingFence());
            };
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
                        new ShardJoinHandler(stages.get(Scenario.SHARDS), shardFences, results ? columns : null), inbound, config);
                processors.add(join);
                final List<IFence> all = new ArrayList<>(List.of(shardFences));
                all.add(join.getReleasingFence());
                return new IFlowProcessorsFactory.ProcessorsChain(processors, new AggregatingMinFence(all));
            };
        };
    }

    private SimpleMessageHandler handler(int stage) {
        final int[] upIds = results ? Bench.upstreamResults(scenario, stage) : new int[0];
        final long[][] upColumns = new long[upIds.length][];
        for (int i = 0; i < upIds.length; i++) {
            upColumns[i] = columns[upIds[i]];
        }
        return new StageHandler(stages.get(stage), columns[stage], upIds, upColumns, ringLongs - 1);
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

    private record StageHandler(Stage stage, long[] column, int[] upIds, long[][] upColumns, int columnMask)
            implements SimpleMessageHandler {
        @Override
        public void handleMessage(long[] buffer, int index, int msgSize, long timestamp, long globalOffset, long correlationId, byte msgType) {
            if (msgType != MSG_TYPE) {
                return;
            }
            final long key = buffer[index];
            if (stage.accepts(key)) {
                final long sum = payloadSum(buffer, index);
                final int c = (int) globalOffset & columnMask;
                for (int u = 0; u < upIds.length; u++) {
                    stage.verify(upIds[u], sum, upColumns[u][c]);
                }
                final long r = stage.process(timestamp, key, sum);
                if (column != null) {
                    column[c] = r;
                }
            }
        }
    }

    /**
     * Conditional fence: waits for the shard that owns the message, not for all shards; bounds already seen are cached.
     */
    private static final class ShardJoinHandler implements SimpleMessageHandler {

        private final Stage stage;
        private final IFence[] shardFences;
        private final long[][] columns;
        private final long[] knownProgress;
        private final long shardMask;

        ShardJoinHandler(Stage stage, IFence[] shardFences, long[][] columns) {
            this.stage = stage;
            this.shardFences = shardFences;
            this.columns = columns;
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
            final long sum = payloadSum(buffer, index);
            if (columns != null) {
                final long[] column = columns[shard];
                stage.verify(shard, sum, column[(int) globalOffset & (column.length - 1)]);
            }
            stage.process(timestamp, key, sum);
        }
    }

    /**
     * PIPELINE3_1T: the three stages as PipelinedFlowProcessor stages on one thread.
     */
    private record PipelinedStage(Stage stage, long[] buffer) implements PipelinedStageHandler<PipelinedFlowSession> {
        @Override
        public boolean process(PipelinedFlowSession session) {
            if (session.messageType == MSG_TYPE) {
                final long key = buffer[session.bufferIndex];
                stage.process(session.timestamp, key, payloadSum(buffer, session.bufferIndex));
            }
            return true;
        }

        @Override
        public int getHitWorkWeight() {
            return 1;
        }
    }
}
