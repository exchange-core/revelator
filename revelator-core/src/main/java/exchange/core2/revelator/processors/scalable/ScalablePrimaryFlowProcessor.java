package exchange.core2.revelator.processors.scalable;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static exchange.core2.revelator.processors.scalable.ScalableShardClassifier.SHARD_ALL;
import static exchange.core2.revelator.processors.scalable.ScalableShardClassifier.SHARD_NONE;

public final class ScalablePrimaryFlowProcessor implements IFlowProcessor {

    private static final Logger log = LoggerFactory.getLogger(ScalablePrimaryFlowProcessor.class);

    // 0..n-2 processors (n-1 is master)
    private final ScalableSecondaryFlowProcessor[] secondaryProcessors;
    private final ScalableMessageHandler[] handlers;

    private final ScalableShardClassifier shardClassifier;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence = new SingleWriterFence();

    private final int indexMask;
    private final long[] buffer;
    private final int bufferSize;

    private volatile PrimaryState actualState = PrimaryState.CENTRALIZED;

    public ScalablePrimaryFlowProcessor(final ScalableMessageHandler[] handlers,
                                        final ScalableSecondaryFlowProcessor[] processors,
                                        final ScalableShardClassifier shardClassifier,
                                        final IFence inboundFence,
                                        final RevelatorConfig config) {

        this.handlers = handlers;
        this.secondaryProcessors = processors;
        this.shardClassifier = shardClassifier;
        this.inboundFence = inboundFence;
        this.indexMask = config.getIndexMask();
        this.buffer = config.getBuffer();
        this.bufferSize = config.getBufferSize();
    }

    @Override
    public void run() {


        long positionSeq = 0L;

        while (true) {

            int queueRefreshCounter = 0;

            long availableSeq;
            while ((availableSeq = inboundFence.getAcquire(positionSeq)) <= positionSeq) {

                if (actualState == PrimaryState.SHARDED) {
                    actualState = PrimaryState.CONSOLIDATING;

                    // no new messages + still in sharded mode => signal secondary processors to initiate stop
                    for (final ScalableSecondaryFlowProcessor p : secondaryProcessors) {
                        //p.deactivate(positionSeq);
                    }
                }
                Thread.onSpinWait();
            }


            // check if batch is big enough to activate secondary processors
            if (actualState == PrimaryState.CENTRALIZED && availableSeq - positionSeq > 8192) { // TODO could be just single large message
                // switch to sharded mode
                actualState = PrimaryState.SHARDED;

                for (final ScalableSecondaryFlowProcessor p : secondaryProcessors) {
                    p.processRange(positionSeq, availableSeq);
                }

                //final int index = (int) (positionSeq & indexMask);
            }

            // TODO separate implementations depending on actualState?

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
                    for (final ScalableMessageHandler handler : handlers) {
                        handler.onShutdown();
                    }
                    return;
                }

                final int indexMsg = index + Revelator.MSG_HEADER_SIZE;

                // payload size in longs
                final int payloadSize = (int) buffer[index + 2];
                if (indexMsg + payloadSize > bufferSize) {
                    throw new IllegalStateException("Failed to decode message: headerSize=" + Revelator.MSG_HEADER_SIZE
                            + " payloadSize=" + payloadSize
                            + " correlationId=" + correlationId
                            + " unexpected " + (indexMsg + payloadSize - bufferSize) + " bytes");
                }

                // check which for which handlers to execute
                final int shard = shardClassifier.getShardMessage(buffer, indexMsg, payloadSize, msgType);

                if (shard != ScalableShardClassifier.SHARD_NONE) {

                    final long timestamp = buffer[index + 1];

                    if (shard == ScalableShardClassifier.SHARD_ALL) {

                        // broadcast to each handler
                        for (final ScalableMessageHandler handler : handlers) {
                            try {
                                handler.handleMessage(buffer, indexMsg, payloadSize, timestamp, positionSeq, correlationId, msgType);
                            } catch (final Exception ex) {
                                log.debug("Exception when processing batch", ex);
                            }
                        }

                    } else {

                        // broadcast to one handler
                        final ScalableMessageHandler handler = handlers[shard];

                        try {
                            handler.handleMessage(buffer, indexMsg, payloadSize, timestamp, positionSeq, correlationId, msgType);
                        } catch (final Exception ex) {
                            log.debug("Exception when processing batch", ex);
                        }
                    }

                }

                positionSeq += Revelator.MSG_HEADER_SIZE + payloadSize;

                // limit batches size
                if (queueRefreshCounter++ > 256) {
                    break;
                }

            }

            releasingFence.setRelease(availableSeq);
        }

    }

    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }

    @Override
    public String toString() {
        return "ScalableFlowProcessor{" + Arrays.toString(handlers) + '}';
    }


    public enum PrimaryState {
        CENTRALIZED,
        //        SHARDED_PREPARE,
        SHARDED,
        CONSOLIDATING,
    }
}
