package exchange.core2.revelator.perf;

/**
 * Pipeline shapes. Threads = consumer threads; with the publisher every scenario fits into 6 isolated cores.
 */
public enum Scenario {

    /** P -> A */
    UNICAST(1),

    /** P -> A -> B -> C */
    PIPELINE3(3),

    /** P -> A,B,C on one thread: Revelator PipelinedFlowProcessor, Disruptor/raw one handler running A, B, C per message */
    PIPELINE3_1T(1),

    /** P -> {A, B, C}, every consumer sees every message */
    MULTICAST3(3),

    /** P -> {A, B} -> C, C waits for both */
    DIAMOND(3),

    /** P -> {S0..S3}, shard i handles keys with key % 4 == i, then J joins: J waits for the shard of each message */
    SHARDED4(5),

    /** P -> N consumers that all see every message, N = -Dperf.readers (1..5, default 3) */
    MULTICAST(-1);

    public static final int SHARDS = 4;

    private final int threads;

    Scenario(int threads) {
        this.threads = threads;
    }

    public int threads() {
        return threads > 0 ? threads : readers();
    }

    static int readers() {
        return Integer.getInteger("perf.readers", 3);
    }
}
