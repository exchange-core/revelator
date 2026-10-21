package exchange.core2.revelator.perf;

/**
 * Pipeline shapes. Threads = consumer threads; with the publisher every scenario fits into 6 isolated cores.
 */
public enum Scenario {

    /** P -> A */
    UNICAST(1),

    /** P -> A -> B -> C */
    PIPELINE3(3),

    /** P -> {A, B, C}, every consumer sees every message */
    MULTICAST3(3),

    /** P -> {A, B} -> C, C waits for both */
    DIAMOND(3),

    /** P -> {S0..S3}, shard i handles keys with key % 4 == i, then J joins: J waits for the shard of each message */
    SHARDED4(5);

    public static final int SHARDS = 4;

    public final int threads;

    Scenario(int threads) {
        this.threads = threads;
    }
}
