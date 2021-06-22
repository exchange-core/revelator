package exchange.core2.revelator.primes;

import exchange.core2.revelator.LatencyTools;
import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.utils.AffinityThreadFactory;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public final class RevelatorPipelineTester {

    private static final Logger log = LoggerFactory.getLogger(RevelatorPipelineTester.class);

    private static final String HMAC_SHA_256 = "HmacSHA256";


    public static void main(String[] args) throws InterruptedException {

        final Random rand = new Random(1L);

        byte[] secretKey = new byte[32];
        rand.nextBytes(secretKey);

        log.info("Secret {}", Arrays.toString(secretKey));

        byte[] hmacSha256 = null;

        hdrRecorder.reset();

        try (final AffinityLock lock = AffinityLock.acquireCore()) {


            final Mac mac = Mac.getInstance(HMAC_SHA_256);
            final SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey, HMAC_SHA_256);
            mac.init(secretKeySpec);
//            hmacSha256 = mac.doFinal(message);

            final byte[] messageArray = new byte[64];


            final AffinityThreadFactory atf = new AffinityThreadFactory(
                    AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE);

            final Revelator r = Revelator.create(
                    bufferSizeTest,
                    ProcessorsFactories.single(RevelatorPipelineTester::handleMessage),
                    atf);

            r.start();

            log.debug("Starting publisher on core: {}", lock.cpuId());

            final int indexMask = r.getIndexMask();

            for (int tps = 1_000_000; tps <= 100_000_000; tps += 200_000 + (rand.nextInt(10000) - 5000)) {

                latch = new CountDownLatch(1);

                final long picosPerCmd = (1024L * 1_000_000_000L) / tps;

                startTimeNs = System.nanoTime();
                final long startTimeMs = System.currentTimeMillis();

                long plannedTimestampPs = 0L;

                long expectedXorData = 0L;

                long lastKnownTimestampPs = 0L;


                int nanoTimeRequestsCounter = 0;
                final long tailStrikesInitial = r.getTailStrike();

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

                    final long correlationId = plannedTimestampPs & 0x00FF_FFFF_FFFF_FFFFL;

//            log.debug("request {}...", i);
                    final long claimSeq = r.claimSingleMessage(testMsgSize, plannedTimestampPs, correlationId, (byte) 1);

                    final int index = (int) (claimSeq & indexMask);


//                    log.debug("WRITE correlationId: {}", correlationId);
//            log.debug("claimSeq={}", claimSeq);
                    long x = 0;

                    for (int k = 0; k < testMsgSize; k += 8) {
//                        log.debug("WRITE data[{}]: {}", k, i);
                        r.writeLongDataUnsafe(index + k, i);
                        x += i;
                    }

                    expectedXorData = x + (expectedXorData ^ correlationId);

//                    log.debug("expectedXorData : {}", expectedXorData);

                    r.publish(claimSeq + testMsgSize);
                }


                //Thread.sleep(10000);

                final long claim = r.claimSingleMessage(8, plannedTimestampPs, iterationsPerTestCycle, END_MARKER);
                r.writeLongData(claim, 0, 0);
                r.publish(claim + 8);

//                log.debug("FINAL PUBLISH DONE");

                final float processingTimeUs = (System.nanoTime() - startTimeNs) / 1000f;
                final float perfMt = (float) iterationsPerTestCycle / processingTimeUs;
                final float targetMt = (float) tps / 1_000_000.0f;
                final String tag = String.format("%.2fns %.3f -> %.3f MT/s %.0f%%",
                        picosPerCmd / 1024.0, targetMt, perfMt, perfMt / targetMt * 100.0);

                latch.await();

                long tailStrikes = r.getTailStrike() - tailStrikesInitial;

                final Histogram histogram = hdrRecorder.getIntervalHistogram();
                final Map<String, String> latencyReportFast = LatencyTools.createLatencyReportFast(histogram);
//                final Map<String, String> latencyReportFast = Map.of();
                log.info("{} {} avg={} nanotimes={} tailStrikes={}", tag, latencyReportFast, (int) avgBatch, nanoTimeRequestsCounter, tailStrikes);

                if (xorData != expectedXorData) {
                    throw new IllegalStateException("Inconsistent messages");
//                    log.debug("Inconsistent messages XOR:{} processedMessages:{}", xorData, processedMessages);
//                } else {
//                    log.debug("XOR:{} processedMessages:{}", xorData, processedMessages);
                }
                xorData = 0L;
                processedMessages = 0;

//            if (histogram.getValueAtPercentile(50) > 10_000_000) {
//                break;
//            }
            }
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            e.printStackTrace();
        }

    }


    private static void handleMessage(long addr,
                                      int msgSize,
                                      long timestamp,
                                      long correlationId,
                                      byte msgType) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

//        double a =  0.0001;
//        avgBatch = avgBatch * (1.0 - a) + msgSize * a;

        if (msgType == END_MARKER) {
//            log.debug("END_MARKER MESSAGE msgSize={}", msgSize);

            latch.countDown();
        } else {

//            log.debug("READ correlationId: {}", correlationId);

            long x = 0;
            for (int k = 0; k < testMsgSize; k += 8) {
                final long data = Revelator.UNSAFE.getLong(addr + k);
                x += data;
//                log.debug("READ data[{}]: {}", k, data);
            }

            xorData = x + (xorData ^ correlationId);

//            log.debug("xorData : {}", xorData);
//                processedMessages++;

//                if ((msg & (0x1F << 6)) == 0) {
//                if (cx++ == 3) {
            if (cx++ == 32) {
                cx = 0;
                final long latency = System.nanoTime() - startTimeNs - (timestamp >> 10);
                hdrRecorder.recordValue(latency);
            }
        }
    }


    final static int testMsgSize = 48; // can be 0

    //    final static int iterationsPerTestCycle = 1488;
    final static int iterationsPerTestCycle = 1_000_000;


    final static int bufferSizeTest = 4 * 1024 * 1024;
    final static SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    static long xorData = 0L;
    static int processedMessages = 0;

    static long startTimeNs = 0L;
    static CountDownLatch latch;
    final static byte END_MARKER = (byte) 7;

    static long c = 0;

    static double avgBatch = 1;

    static int cx = 0;


}
