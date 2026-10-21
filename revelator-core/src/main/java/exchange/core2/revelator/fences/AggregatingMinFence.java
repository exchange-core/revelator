package exchange.core2.revelator.fences;

import java.util.List;

public final class AggregatingMinFence implements IFence {


    public AggregatingMinFence(IFence[] fences) {
        this.fences = fences;
    }

    public AggregatingMinFence(List<IFence> fences) {
        this.fences = fences.toArray(IFence[]::new);
    }

    private final IFence[] fences;

    // TODO remember last rejected fence and start check from that number (such implementation is not thread safe though)

    @Override
    public long getAcquire(final long lastKnown) {

        long min = Long.MAX_VALUE;

        for (final IFence fence : fences) {
            final long seq = fence.getAcquire(lastKnown);

            if (seq <= lastKnown) {
                // no need to check remaining fences - no progress can be made anyway
                return seq;
            }
            min = Math.min(min, seq);
        }

        return min;
    }

    @Override
    public long getVolatile() {
        long min = Long.MAX_VALUE;

        for (final IFence fence : fences) {
            final long seq = fence.getVolatile();
            min = Math.min(min, seq);
        }

        return min;
    }

    @Override
    public long getOpaque() {
        long min = Long.MAX_VALUE;

        for (final IFence fence : fences) {
            final long seq = fence.getOpaque();
            min = Math.min(min, seq);
        }

        return min;
    }


}
