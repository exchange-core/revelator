package exchange.core2.revelator.fences;

public interface IFence {

    long getAcquire(long lastKnown);

    long getVolatile();

    long getOpaque();

}
