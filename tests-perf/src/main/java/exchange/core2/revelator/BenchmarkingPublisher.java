package exchange.core2.revelator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkingPublisher {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkingPublisher.class);


    public static void main(String[] args) {


        long x = 0;

        int targetDelayPs = 6_000;
        int g = targetDelayPs / 1000;

        for (int i = 0; i < 1000; i++) {

            long startTime = System.nanoTime();
            for (int r = 0; r < 1000; r++) {
                for (int j = 0; j < g; j++) {
//                    x = (x << 1) + j;
                    x += j;
                }
            }

            final long durationPs = (System.nanoTime() - startTime);

            double ratio = (double) targetDelayPs / durationPs;

            log.debug("{}. duration={}ps g={} ratio={}", i, durationPs, g, ratio);

            double ratioNorm = Math.max(Math.min(ratio, 3), 0.5);

            g = Math.max(1, (int) (g * ratioNorm));


//            if (durationNanos < targetDelayNs) {
//                g += Math.max(g >> 7, 1);
//            } else {
//                g -= Math.max(g >> 7, 1);
//            }


        }


        log.debug("g={} ({})", g, x);

    }
}
