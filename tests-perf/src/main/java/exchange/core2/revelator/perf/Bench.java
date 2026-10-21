package exchange.core2.revelator.perf;

import java.util.List;

/**
 * One framework wired into one scenario. Only one implementation is loaded per JVM, so every call site stays
 * monomorphic for both frameworks.
 */
public interface Bench {

    /** Starts consumer threads (created through the given factory, in stage order). */
    void start(PinnedThreadFactory threadFactory);

    /** Publishes one message: timestamp plus a 6-long payload key, key+1 .. key+5. */
    void publish(long timestamp, long key);

    /** All stages in thread order; terminal ones record latency and are checked for completion. */
    List<Stage> stages();

    static List<Stage> createStages(Scenario scenario, int work) {
        return switch (scenario) {
            case UNICAST -> List.of(
                    new Stage("A", 0, -1, 1, work, true));
            case PIPELINE3, PIPELINE3_1T, DIAMOND -> List.of(
                    new Stage("A", 0, -1, 1, work, false),
                    new Stage("B", 1, -1, 1, work, false),
                    new Stage("C", 2, -1, 1, work, true));
            case MULTICAST3 -> List.of(
                    new Stage("A", 0, -1, 1, work, true),
                    new Stage("B", 1, -1, 1, work, true),
                    new Stage("C", 2, -1, 1, work, true));
            case SHARDED4 -> List.of(
                    new Stage("S0", 0, 0, Scenario.SHARDS, work, false),
                    new Stage("S1", 1, 1, Scenario.SHARDS, work, false),
                    new Stage("S2", 2, 2, Scenario.SHARDS, work, false),
                    new Stage("S3", 3, 3, Scenario.SHARDS, work, false),
                    new Stage("J", 4, -1, 1, work, true));
        };
    }

    /**
     * Stages whose result a stage reads in -Dperf.results mode (the SHARDED4 join reads the owner shard instead).
     */
    static int[] upstreamResults(Scenario scenario, int stage) {
        return switch (scenario) {
            case PIPELINE3 -> stage == 0 ? new int[0] : new int[]{stage - 1};
            case DIAMOND -> stage == 2 ? new int[]{0, 1} : new int[0];
            default -> new int[0];
        };
    }
}
