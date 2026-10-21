package exchange.core2.revelator.fences;

public class ScalableFenceArray implements IFenceArray {


    private final long mask;
    private final IFence[] fences;

    // TODO add cache

    public ScalableFenceArray(final IFence[] fences, int numHandlers) {
        this.fences = fences;
        this.mask = numHandlers - 1;
    }


    @Override
    public long getAcquire(long entityId) {

        return fences[(int)(entityId & mask)].getAcquire(0L);
    }

    @Override
    public long getVolatile(long entityId) {
        return 0;
    }

    @Override
    public long getOpaque(long entityId) {
        return 0;
    }
}
