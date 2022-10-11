package exchange.core2.revelator.processors.scalable;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;

public final class ScalableSecondaryFlowProcessor implements IFlowProcessor {

    private static final Logger log = LoggerFactory.getLogger(ScalableSecondaryFlowProcessor.class);

    private final SimpleMessageHandler handler;

    private final SingleWriterFence releasingFence = new SingleWriterFence();

    private final int indexMask;
    private final long[] buffer;
    private final int bufferSize;

    private final BlockingQueue<LongLongPair> tasks = new ArrayBlockingQueue<>(32);


    public ScalableSecondaryFlowProcessor(final SimpleMessageHandler handler,
                                          final RevelatorConfig config) {

        this.handler = handler;
        this.indexMask = config.getIndexMask();
        this.buffer = config.getBuffer();
        this.bufferSize = config.getBufferSize();
    }

    @Override
    public void run() {
        try {
            while (true) {

                final LongLongPair task = tasks.take();

                long positionSeq = task.getOne();
                final long availableSeq = task.getTwo();

                while (positionSeq < availableSeq) {

                    final int index = (int) (positionSeq & indexMask);

                    final long header1 = buffer[index];

                    if (header1 == 0L) {
                        // skip until end of the buffer
                        positionSeq = (positionSeq | indexMask) + 1;
                        continue;
                    }

                    final long correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;
                    final int header2 = (int) (header1 >>> 56);
                    final byte msgType = (byte) (header2 & 0x1F);

                    if (msgType == Revelator.MSG_TYPE_POISON_PILL) {
                        log.debug("processor shutdown (received msgType={}, publishing positionSeq={}+{})", msgType, positionSeq, Revelator.MSG_HEADER_SIZE);
                        releasingFence.setRelease(positionSeq + Revelator.MSG_HEADER_SIZE);
                        handler.onShutdown();
                        return;
                    }

                    final long timestamp = buffer[index + 1];

                    // payload size in longs
                    final int payloadSize = (int) buffer[index + 2];
                    final int indexMsg = index + Revelator.MSG_HEADER_SIZE;
                    if (indexMsg + payloadSize > bufferSize) {
                        throw new IllegalStateException("Failed to decode message: headerSize=" + Revelator.MSG_HEADER_SIZE
                                + " payloadSize=" + payloadSize
                                + " correlationId=" + correlationId
                                + " unexpected " + (indexMsg + payloadSize - bufferSize) + " bytes");
                    }

                    try {
                        handler.handleMessage(buffer, indexMsg, payloadSize, timestamp, positionSeq, correlationId, msgType);
                    } catch (final Exception ex) {
                        log.debug("Exception when processing batch", ex);
                        // TODO call custom handler
                    }

                    positionSeq += Revelator.MSG_HEADER_SIZE + payloadSize;
                }

                releasingFence.setRelease(availableSeq);
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void processRange(long offsetFrom, long offsetTo) {
        try {
            tasks.put(PrimitiveTuples.pair(offsetFrom, offsetTo));
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }

    @Override
    public String toString() {
        return "ScalableFlowProcessor{" + handler + '}';
    }
}
