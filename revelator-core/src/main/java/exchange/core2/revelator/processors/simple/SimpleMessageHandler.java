package exchange.core2.revelator.processors.simple;

public interface SimpleMessageHandler {

    /**
     * Allows batching
     * Can have wrapping
     *
     * @param msgSize messageSize
     */
    void handleMessage(long[] buffer, // TODO use off-heap memory region
                       int index,
                       int msgSize,
                       long timestamp,
                       long globalOffset,
                       long correlationId,
                       byte msgType);

    default void onShutdown() {
    }

}
