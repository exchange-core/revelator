package exchange.core2.revelator.examples;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.processors.ProcessorsFactories;

/** Run with the revelator-core jar and its dependencies on the classpath. */
public final class BasicExample {
    public static void main(String[] args) {
        long[] total = {0};
        try (Revelator runtime =
                Revelator.create(
                        1024,
                        ProcessorsFactories.single(
                                (buffer, index, size, time, start, id, type) ->
                                        total[0] += buffer[index]),
                        Thread::new)) {
            runtime.start();
            for (int i = 1; i <= 100; i++) {
                long payload = runtime.claimSingleMessage(1, System.nanoTime(), i, (byte) 1);
                runtime.writeLongData(payload, 0, i);
                runtime.publish(payload + 1);
            }
        } // drains the published batch and joins consumers
        System.out.println("total=" + total[0]);
    }
}
