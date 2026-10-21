package exchange.core2.revelator.processors.scalable;

public interface ScalableMessageHandler {

    void handleMessage(long[] buffer,
                       int payloadIndex,
                       int payloadSize,
                       long timestamp,
                       long globalOffset,
                       long correlationId,
                       byte msgType);




    default void onShutdown() {
    }

}
