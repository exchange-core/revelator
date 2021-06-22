package exchange.core2.revelator.fences;

import exchange.core2.revelator.processors.IFlowProcessor;
import jdk.internal.vm.annotation.Contended;

import java.util.Arrays;
import java.util.List;
import java.util.function.LongToIntFunction;

public final class ShardedFence {

    public static ShardedFence create(final List<IFlowProcessor> processors,
                                      final LongToIntFunction shardFunction) {


        final IFence[] fences = processors.stream()
                .map(IFlowProcessor::getReleasingFence)
                .toArray(IFence[]::new);

        final long[] lastKnown = new long[fences.length];
        Arrays.fill(lastKnown, -1);

        return new ShardedFence(fences, shardFunction, lastKnown);
    }

    public ShardedFence(final IFence[] fences,
                        final LongToIntFunction shardFunction,
                        final long[] lastKnown) {

        this.fences = fences;
        this.shardFunction = shardFunction;
        this.lastKnown = lastKnown;
    }

    private final IFence[] fences;

    private final LongToIntFunction shardFunction;

    @Contended
    private final long[] lastKnown;


    public long getForId(final long id) {

        final int fenceId = shardFunction.applyAsInt(id);
        final IFence fence = fences[fenceId];
        final long seq = fence.getVolatile(lastKnown[fenceId]);
        lastKnown[fenceId] = seq;
        return seq;
    }

}
