package exchange.core2.revelator.utils;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public final class LatencyTesterModule {

    private final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    public interface TestingHandler {
        void initializeIteration(long startTimeNs, Runnable lastMessageNotifier);

        void sendMessage(long plannedTimestampPs, int iteration);

        void sendFinal(long plannedTimestampPs);

        void createReport(String tag,
                          Map<String, String> latencyReportFast,
                          long nanoTimeRequestsCounter);
    }


    public void performTests(final int iterationsPerTestCycle,
                             final int minTps,
                             final int endTps,
                             final int tpsIncrement,
                             final int tpsJitter,
                             final TestingHandler testingHandler) {


        final Random rand = new Random(1L);


        for (int tps = minTps; tps <= endTps; tps += tpsIncrement + (rand.nextInt(tpsJitter) - tpsJitter/2)) {

            hdrRecorder.reset();

            final CountDownLatch latch = new CountDownLatch(1);

            final long picosPerCmd = (1024L * 1_000_000_000L) / tps;

            final long startTimeNs = System.nanoTime();

            testingHandler.initializeIteration(startTimeNs, latch::countDown);

            // final long startTimeMs = System.currentTimeMillis();

            long plannedTimestampPs = 0L;


            long lastKnownTimestampPs = 0L;


            int nanoTimeRequestsCounter = 0;

            for (int i = 0; i < iterationsPerTestCycle; i++) {

                plannedTimestampPs += picosPerCmd;

                while (plannedTimestampPs > lastKnownTimestampPs) {

                    lastKnownTimestampPs = (System.nanoTime() - startTimeNs) << 10;

                    nanoTimeRequestsCounter++;

                    // spin until its time to send next command
//                        Thread.onSpinWait(); // 1us-26  max34
//                        LockSupport.parkNanos(1L); // 1us-25 max29
//                         Thread.yield();   // 1us-28  max32
                }

                testingHandler.sendMessage(plannedTimestampPs, i);
            }


            //Thread.sleep(10000);


            testingHandler.sendFinal(plannedTimestampPs);

//                log.debug("FINAL PUBLISH DONE");

            final float processingTimeUs = (System.nanoTime() - startTimeNs) / 1000f;
            final float perfMt = (float) iterationsPerTestCycle / processingTimeUs;
            final float targetMt = (float) tps / 1_000_000.0f;
            final String tag = String.format("%.2fns %.3f -> %.3f MT/s %.0f%%",
                    picosPerCmd / 1024.0, targetMt, perfMt, perfMt / targetMt * 100.0);

            try {
                latch.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);

            }

            final Histogram histogram = hdrRecorder.getIntervalHistogram();
            final Map<String, String> latencyReportFast = LatencyTools.createLatencyReportFast(histogram);


            testingHandler.createReport(tag, latencyReportFast, nanoTimeRequestsCounter);

//            if (histogram.getValueAtPercentile(50) > 10_000_000) {
//                break;
//            }
        }

    }

    public SingleWriterRecorder getHdrRecorder() {
        return hdrRecorder;
    }
}
