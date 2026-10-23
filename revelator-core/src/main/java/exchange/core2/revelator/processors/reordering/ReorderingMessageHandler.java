package exchange.core2.revelator.processors.reordering;

/**
 * Handler that may postpone a message: ReorderingFlowProcessor then goes on with later messages whose signatures do
 * not intersect the postponed ones and comes back to it later.
 */
public interface ReorderingMessageHandler {

    /**
     * Ordering keys of the message as a bit set (e.g. one bit chosen by a hash of each account it touches). Two
     * messages whose signatures intersect are always handled in log order; others may be handled in any order.
     * A signature of -1 orders the message against everything.
     */
    long signature(long[] buffer, int index, int msgSize, byte msgType);

    /**
     * @return false if the message cannot be handled yet (a dependency is not ready); it will be offered again
     */
    boolean tryHandle(long[] buffer, int index, int msgSize, long timestamp, long globalOffset, long correlationId, byte msgType);

    default void onShutdown() {
    }
}
