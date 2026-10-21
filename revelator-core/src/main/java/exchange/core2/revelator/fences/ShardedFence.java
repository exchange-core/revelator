package exchange.core2.revelator.fences;

import exchange.core2.revelator.processors.IFlowProcessor;

import java.util.Arrays;
import java.util.List;
import java.util.function.LongToIntFunction;

/**
 * Not thread-safe.
 * Each consumer should have its own copy.
 */
public final class ShardedFence {

    public static ShardedFence createFromProcessors(final List<IFlowProcessor> processors,
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

    private final long[] lastKnown;


    public long getForId(final long id) {

        final int fenceId = shardFunction.applyAsInt(id);
        final IFence fence = fences[fenceId];
        final long seq = fence.getAcquire(lastKnown[fenceId]);
        lastKnown[fenceId] = seq;
        return seq;
    }

}
