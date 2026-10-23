package exchange.core2.revelator.processors.simple;

import exchange.core2.revelator.FrameLayout;
import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.HeaderCommitFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SimpleFlowProcessor implements IFlowProcessor {

    private static final Logger log = LoggerFactory.getLogger(SimpleFlowProcessor.class);

    private final SimpleMessageHandler handler;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence = new SingleWriterFence();

    private final int indexMask;
    private final long[] buffer;
    private final int bufferSize;
    private final FrameLayout frameLayout;

    // release consumed space at least this often (in longs): with an unbounded batch a full ring was released only
    // after processing all of it, so publisher and consumer took turns instead of running in parallel
    private static final int RELEASE_EVERY = Integer.getInteger("revelator.releaseEvery", 4096);

    // header-commit loop: progress is published at least this often (messages), as often as HeaderCommitFence scans
    private static final int HEADER_RELEASE_EVERY = 64;

    private long superCounter;

    public SimpleFlowProcessor(final SimpleMessageHandler handler,
                               final IFence inboundFence,
                               final RevelatorConfig config) {

        this.handler = handler;
        this.inboundFence = inboundFence;
        this.indexMask = config.getIndexMask();
        this.buffer = config.getBuffer();
        this.bufferSize = config.getBufferSize();
        this.frameLayout = config.getFrameLayout();
    }

    @Override
    public void run() {

        if (inboundFence instanceof HeaderCommitFence) {
            runHeaderCommit();
            return;
        }

        long positionSeq = 0L;
        long releasedSeq = 0L;

        while (true) {

            long availableSeq;
            int c = 0;
            while ((availableSeq = inboundFence.getAcquire(positionSeq)) <= positionSeq) {
                Thread.onSpinWait();
//                LockSupport.parkNanos(1L);
//                    if (c++ == 100) {
//                        Thread.yield();
//                        c = 0;
//                    }
//                for (int i = 0; i < 40; i++) {
//                    superCounter = (superCounter << 1) + superCounter;
//                }
//                superCounter += System.nanoTime();

            }

//        log.debug("Handle batch bufAddr={} positionSeq={} availableSeq={}", bufferAddr, positionSeq, availableSeq);

            while (positionSeq < availableSeq) {

//            log.debug("positionSeq={} availableSeq={}", positionSeq, availableSeq);
//            log.debug("reading at index={} ({}<{})",(int) (positionSeq & indexMask), positionSeq , availableSeq);

                final int index = (int) (positionSeq & indexMask);

                final long header1 = buffer[index];

//                log.debug("SIMPLE positionSeq={} availableSeq={} header1={}", positionSeq, availableSeq, header1);

                if (header1 == 0L) {
                    // skip until end of the buffer

                    final long endOfLoopSeq = frameLayout.nextLap(positionSeq, indexMask);
//                log.debug("Zero message: SKIP positionSeq={} -> endOfLoopSeq={} ({} bytes)", positionSeq, endOfLoopSeq, endOfLoopSeq - positionSeq);
                    positionSeq = endOfLoopSeq;
                    continue;
                }


                final long correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;
                final int header2 = (int) (header1 >>> 56);

//            log.debug("{}", String.format("header1=%X", header1));
//            log.debug("{}", String.format("correlationId=%X = %d", correlationId, correlationId));
//            log.debug("{}", String.format("header2=%X", header2));

                final byte msgType = (byte) (header2 & 0x1F);

                if (msgType == Revelator.MSG_TYPE_POISON_PILL) {

                    log.debug("processor shutdown (received msgType={}, publishing positionSeq={}+{})", msgType, positionSeq, Revelator.MSG_HEADER_SIZE);
                    releasingFence.setRelease(positionSeq + Revelator.MSG_HEADER_SIZE);

                    handler.onShutdown();
                    return;

                } else {

//            log.debug("{}", String.format("msgSizeLongsCompact=%X", msgSizeLongsCompact));
//            log.debug("{}", String.format("msgType=%X", msgType));

                    final long timestamp = buffer[index + 1];
//            log.debug("timestamp={}", timestamp);

                    // payload size in longs
                    final int payloadSize = (int) buffer[index + 2];
//                log.debug("custom payloadSize={}", payloadSize);

                    final int indexMsg = index + Revelator.MSG_HEADER_SIZE;
                    if (indexMsg + payloadSize > bufferSize) {
                        throw new IllegalStateException("Failed to decode message: headerSize=" + Revelator.MSG_HEADER_SIZE
                                + " payloadSize=" + payloadSize
                                + " correlationId=" + correlationId
                                + " unexpected " + (indexMsg + payloadSize - bufferSize) + " bytes");
                    }


                    try {
//                log.debug("Handle message messageStartAddress={} -> offsetInBuf={} payloadSize={}",
//                        messageStartAddress, headerStartAddress - bufferAddr, payloadSize);

//                Thread.sleep(1);
                        handler.handleMessage(buffer, indexMsg, payloadSize, timestamp, positionSeq, correlationId, msgType);
//                log.debug("DONE");
                    } catch (final Exception ex) {
                        log.debug("Exception when processing batch", ex);
                        // TODO call custom handler
                    }

//            log.debug("positionSeq: {}->{} ", positionSeq, positionSeq + headerSize + payloadSize );

//                    releasingFence.setRelease(positionSeq);

                    positionSeq = frameLayout.nextFrame(positionSeq + Revelator.MSG_HEADER_SIZE + payloadSize);

                    if (positionSeq - releasedSeq >= RELEASE_EVERY) {
                        releasingFence.setRelease(positionSeq);
                        releasedSeq = positionSeq;
                    }
                }

//                releasingFence.setRelease(positionSeq);
            }

//            log.debug("RELEASE {}", availableSeq);
            releasingFence.setRelease(availableSeq);
            releasedSeq = availableSeq;
        }

    }

    /**
     * Reads the producer's messages in one pass: checks the commit word of the next message and handles it right away
     * (scanning commit words ahead and then reading the same headers again cost a second pass over every message).
     * Progress is published when no committed message follows, and at least every HEADER_RELEASE_EVERY messages.
     */
    private void runHeaderCommit() {

        long positionSeq = 0L;
        long releasedSeq = 0L;
        int unreleased = 0;

        while (true) {

            final int index = (int) (positionSeq & indexMask);

            if (bufferSize - index < Revelator.MSG_HEADER_SIZE) {
                // implicit padding: no header fits before the end of the ring
                positionSeq = frameLayout.nextLap(positionSeq, indexMask);
                continue;
            }

            if (!HeaderCommitFence.isCommitted(buffer, index, positionSeq)) {
                if (positionSeq != releasedSeq) {
                    releasingFence.setRelease(positionSeq);
                    releasedSeq = positionSeq;
                    unreleased = 0;
                }
                Thread.onSpinWait();
                continue;
            }

            final long header1 = buffer[index];

            if (header1 == 0L) {
                // padding header: skip to the next lap
                positionSeq = frameLayout.nextLap(positionSeq, indexMask);
                continue;
            }

            final long correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;
            final byte msgType = (byte) ((int) (header1 >>> 56) & 0x1F);

            if (msgType == Revelator.MSG_TYPE_POISON_PILL) {
                log.debug("processor shutdown (received msgType={}, publishing positionSeq={}+{})", msgType, positionSeq, Revelator.MSG_HEADER_SIZE);
                releasingFence.setRelease(positionSeq + Revelator.MSG_HEADER_SIZE);
                handler.onShutdown();
                return;
            }

            final long timestamp = buffer[index + 1];
            final int payloadSize = (int) buffer[index + 2];
            final int indexMsg = index + Revelator.MSG_HEADER_SIZE;
            if (indexMsg + payloadSize > bufferSize) {
                throw new IllegalStateException("Failed to decode message: payloadSize=" + payloadSize
                        + " correlationId=" + correlationId + " at " + positionSeq);
            }

            try {
                handler.handleMessage(buffer, indexMsg, payloadSize, timestamp, positionSeq, correlationId, msgType);
            } catch (final Exception ex) {
                log.debug("Exception when processing message", ex);
            }

            positionSeq = frameLayout.nextFrame(positionSeq + Revelator.MSG_HEADER_SIZE + payloadSize);

            if (++unreleased == HEADER_RELEASE_EVERY) {
                releasingFence.setRelease(positionSeq);
                releasedSeq = positionSeq;
                unreleased = 0;
            }
        }
    }

    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }

    @Override
    public String toString() {
        return "SimpleFlowProcessor{" + handler + '}';
    }
}
