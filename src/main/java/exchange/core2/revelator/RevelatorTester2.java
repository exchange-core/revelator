package exchange.core2.revelator;

import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.utils.AffinityThreadFactory;
import exchange.core2.revelator.utils.LatencyTesterModule;
import exchange.core2.collections.affinity.CpuAffinity;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class RevelatorTester2 implements LatencyTesterModule.TestingHandler {

    private static final Logger log = LoggerFactory.getLogger(RevelatorTester2.class);

    public static void main(String[] args) throws InterruptedException {

        RevelatorTester2 revelatorTester = new RevelatorTester2();
        revelatorTester.runTest();
    }

    public RevelatorTester2() {

        final AffinityThreadFactory atf = new AffinityThreadFactory(
                AffinityThreadFactory.ThreadAffinityMode.AFFINITY_LOGICAL_CORE);

        this.revelator = Revelator.create(
                bufferSizeTest,
                ProcessorsFactories.single(this::handleMessage),
                atf);

    }

    public void runTest() {

        revelator.start();


        LatencyTesterModule latencyTesterModule = new LatencyTesterModule();

        hdrRecorder = latencyTesterModule.getHdrRecorder();

        try (final CpuAffinity lock = CpuAffinity.acquireCore()) {

            log.debug("Starting publisher on core: {}", lock.cpu());

            final int iterationsPerTestCycle = 1_000_000;
            latencyTesterModule.performTests(
                    iterationsPerTestCycle, 1_000_000, 80_000_000, 500_000, 20_000, this);
        }

    }

    @Override
    public void initializeIteration(long startTimeNs2, Runnable lastMessageNotifier) {
        startTimeNs = startTimeNs2;
        expectedXorData = 0L;
        xorData = 0L;
        tailStrikesInitial = revelator.getTailStrike();
        this.lastMessageNotifier = lastMessageNotifier;
    }

    @Override
    public void sendMessage(final long plannedTimestampPs, final int i) {

        final long correlationId = plannedTimestampPs & 0x00FF_FFFF_FFFF_FFFFL;
        final long claimSeq = revelator.claimSingleMessage(testMsgSize, plannedTimestampPs, correlationId, (byte) 1);

//        revelator.writeLongData(claimSeq, 0, i, i + 1, i + 2, i + 3, i + 4, i + 5);
//        long x = i * 6L + 15;

        long x = 0;
        for (int k = 0; k < testMsgSize; k++) {
            revelator.writeLongData(claimSeq, k, i);
            x += i;
        }


        expectedXorData = x + (expectedXorData ^ correlationId);
        revelator.publish(claimSeq + testMsgSize);
    }

    public void sendFinal(long plannedTimestampPs) {

        final long claim = revelator.claimSingleMessage(8, plannedTimestampPs, 0, END_BATCH_MARKER);
        revelator.writeLongData(claim, 0, 0);
        revelator.publish(claim + 8);
    }

    @Override
    public void createReport(String tag,
                             final Map<String, String> latencyReportFast,
                             long nanoTimeRequestsCounter) {

        long tailStrikes = revelator.getTailStrike() - tailStrikesInitial;

        log.info("{} {} nanotimes={} tailStrikes={}", tag, latencyReportFast, nanoTimeRequestsCounter, tailStrikes);

        if (xorData != expectedXorData) {
            throw new IllegalStateException("Inconsistent messages");
//                    log.debug("Inconsistent messages XOR:{} processedMessages:{}", xorData, processedMessages);
//                } else {
//                    log.debug("XOR:{} processedMessages:{}", xorData, processedMessages);
        }

    }

    private void handleMessage(long[] buffer,
                               int index,
                               int msgSize,
                               long timestamp,
                               long globalOffset,
                               long correlationId,
                               byte msgType) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

//        double a =  0.0001;
//        avgBatch = avgBatch * (1.0 - a) + msgSize * a;

        if (msgType == END_BATCH_MARKER) {
//            log.debug("END_MARKER MESSAGE msgSize={}", msgSize);

            lastMessageNotifier.run();
        } else {

//            log.debug("READ correlationId: {}", correlationId);

            long x = 0;
            for (int k = 0; k < testMsgSize; k++) {
                final long data = buffer[index + k];
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

    private final Revelator revelator;

    private Runnable lastMessageNotifier;

    private SingleWriterRecorder hdrRecorder;


    final static int testMsgSize = 6; // can be 0
    final static int bufferSizeTest = 4 * 1024 * 1024;

    long expectedXorData = 0L;

    long xorData = 0L;

    long tailStrikesInitial;

    int cx = 0;

    long startTimeNs = 0L;

    final static byte END_BATCH_MARKER = (byte) 7;


}
