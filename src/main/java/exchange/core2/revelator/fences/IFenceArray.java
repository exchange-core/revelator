package exchange.core2.revelator.fences;

public interface IFenceArray {

    long getAcquire(long entityId);

    long getVolatile(long entityId);

    long getOpaque(long entityId);

}
