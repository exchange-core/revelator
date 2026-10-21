package exchange.core2.revelator.processors.scalable;

public interface ScalableShardClassifier {

    /**
     * @return handlerId (extract shard)
     */
    int getShardMessage(long[] buffer,
                        int payloadIndex,
                        int payloadSize,
                        byte msgType);

    /**
     * Constants
     */
    int SHARD_ALL = -1;
    int SHARD_NONE = Integer.MIN_VALUE;
}
