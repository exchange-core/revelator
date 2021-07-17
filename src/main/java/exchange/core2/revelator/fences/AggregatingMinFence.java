package exchange.core2.revelator.fences;

public final class AggregatingMinFence implements IFence {


    public AggregatingMinFence(IFence[] fences) {
        this.fences = fences;
    }

    private final IFence[] fences;

    // TODO remember last rejected fence and start check from that number (such implementation is not thread safe though)

    @Override
    public long getVolatile(final long lastKnown) {

        long min = Long.MAX_VALUE;

        for (final IFence fence : fences) {
            final long seq = fence.getVolatile(lastKnown);

            if (seq <= lastKnown) {
                // no need to check remaining fences - no progress can be made anyway
                return seq;
            }
            min = Math.min(min, seq);
        }

        return min;
    }


}