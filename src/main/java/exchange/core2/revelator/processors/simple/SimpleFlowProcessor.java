package exchange.core2.revelator.processors.simple;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
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

    private long superCounter;

    public SimpleFlowProcessor(final SimpleMessageHandler handler,
                               final IFence inboundFence,
                               final RevelatorConfig config) {

        this.handler = handler;
        this.inboundFence = inboundFence;
        this.indexMask = config.getIndexMask();
        this.buffer = config.getBuffer();
        this.bufferSize = config.getBufferSize();
    }

    @Override
    public void run() {

        long positionSeq = 0L;

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

                if (header1 == 0L) {
                    // skip until end of the buffer

                    final long endOfLoopSeq = (positionSeq | indexMask) + 1;
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
                    return;

                } else {

//            log.debug("{}", String.format("msgSizeLongsCompact=%X", msgSizeLongsCompact));
//            log.debug("{}", String.format("msgType=%X", msgType));

                    final long timestamp = buffer[index + 1];

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

                    positionSeq += Revelator.MSG_HEADER_SIZE + payloadSize;
                }
            }

//            log.debug("RELEASE {}", availableSeq);
            releasingFence.setRelease(availableSeq);
        }

    }

    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }
}
