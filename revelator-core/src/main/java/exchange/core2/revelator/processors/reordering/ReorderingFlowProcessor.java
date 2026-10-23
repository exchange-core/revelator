package exchange.core2.revelator.processors.reordering;

import exchange.core2.revelator.FrameLayout;
import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles messages out of log order when that cannot be observed: a message the handler cannot take yet is postponed
 * together with its key signature, and later messages go ahead unless their signature intersects a postponed one
 * (then they are postponed too, keeping their order). Postponed messages are offered again, oldest first, on every
 * turn of the loop. The releasing fence never passes the oldest postponed message, so its data stays in the ring.
 * <p>
 * Removes head-of-line blocking behind a slow dependency for messages that do not share keys with the blocked ones.
 * The result is deterministic only if the handler's effects on different keys commute.
 */
public final class ReorderingFlowProcessor implements IFlowProcessor {

    private static final Logger log = LoggerFactory.getLogger(ReorderingFlowProcessor.class);

    private static final int PUBLISH_EVERY = 64; // messages; progress is also published whenever input runs dry

    private final ReorderingMessageHandler handler;
    private final IFence inboundFence;
    private final SingleWriterFence releasingFence = new SingleWriterFence();

    private final int indexMask;
    private final long[] buffer;
    private final int bufferSize;
    private final FrameLayout frameLayout;

    // postponed messages, oldest first
    private final long[] postponedPosition;
    private final long[] postponedSignature;
    private int postponed = 0;

    public ReorderingFlowProcessor(final ReorderingMessageHandler handler,
                                   final IFence inboundFence,
                                   final RevelatorConfig config,
                                   final int maxPostponed) {
        this.handler = handler;
        this.inboundFence = inboundFence;
        this.indexMask = config.getIndexMask();
        this.buffer = config.getBuffer();
        this.bufferSize = config.getBufferSize();
        this.frameLayout = config.getFrameLayout();
        this.postponedPosition = new long[maxPostponed];
        this.postponedSignature = new long[maxPostponed];
    }

    @Override
    public void run() {

        long positionSeq = 0L;
        long available = 0L;
        long released = 0L;
        int sincePublish = 0;

        while (true) {

            final long blocked = postponed == 0 ? 0L : retryPostponed();

            if (positionSeq >= available) {
                available = inboundFence.getAcquire(positionSeq);
                if (available <= positionSeq) {
                    final long progress = progress(positionSeq);
                    if (progress != released) {
                        releasingFence.setRelease(progress);
                        released = progress;
                        sincePublish = 0;
                    }
                    Thread.onSpinWait();
                    continue;
                }
            }

            final int index = (int) (positionSeq & indexMask);
            if (bufferSize - index < Revelator.MSG_HEADER_SIZE || buffer[index] == 0L) {
                positionSeq = frameLayout.nextLap(positionSeq, indexMask); // padding
                continue;
            }

            final long header1 = buffer[index];
            final byte msgType = (byte) ((int) (header1 >>> 56) & 0x1F);
            final int payloadSize = (int) buffer[index + 2];

            if (msgType == Revelator.MSG_TYPE_POISON_PILL) {
                if (postponed != 0) {
                    Thread.onSpinWait(); // finish the postponed messages first
                    continue;
                }
                log.debug("processor shutdown at {}", positionSeq);
                releasingFence.setRelease(positionSeq + Revelator.MSG_HEADER_SIZE);
                handler.onShutdown();
                return;
            }

            if (postponed == postponedPosition.length) {
                Thread.onSpinWait(); // no room to postpone more: wait for the postponed ones
                continue;
            }

            final long signature = handler.signature(buffer, index + Revelator.MSG_HEADER_SIZE, payloadSize, msgType);
            if ((signature & blocked) != 0 || !handle(index, positionSeq, header1, payloadSize, msgType)) {
                postponedPosition[postponed] = positionSeq;
                postponedSignature[postponed] = signature;
                postponed++;
            }

            positionSeq = frameLayout.nextFrame(positionSeq + Revelator.MSG_HEADER_SIZE + payloadSize);

            if (++sincePublish == PUBLISH_EVERY) {
                final long progress = progress(positionSeq);
                releasingFence.setRelease(progress);
                released = progress;
                sincePublish = 0;
            }
        }
    }

    // everything before the oldest postponed message is handled
    private long progress(final long positionSeq) {
        return postponed == 0 ? positionSeq : postponedPosition[0];
    }

    /**
     * Offers postponed messages again in log order; a message is not offered while an older one with an intersecting
     * signature is still waiting.
     *
     * @return signatures of the messages still postponed
     */
    private long retryPostponed() {
        long stillBlocked = 0L;
        int kept = 0;
        for (int i = 0; i < postponed; i++) {
            final long position = postponedPosition[i];
            final long signature = postponedSignature[i];
            if ((signature & stillBlocked) == 0) {
                final int index = (int) (position & indexMask);
                final long header1 = buffer[index];
                if (handle(index, position, header1, (int) buffer[index + 2], (byte) ((int) (header1 >>> 56) & 0x1F))) {
                    continue;
                }
            }
            stillBlocked |= signature;
            postponedPosition[kept] = position;
            postponedSignature[kept] = signature;
            kept++;
        }
        postponed = kept;
        return stillBlocked;
    }

    private boolean handle(final int index, final long position, final long header1, final int payloadSize, final byte msgType) {
        try {
            return handler.tryHandle(buffer, index + Revelator.MSG_HEADER_SIZE, payloadSize, buffer[index + 1], position,
                    header1 & 0x00FF_FFFF_FFFF_FFFFL, msgType);
        } catch (final Exception ex) {
            log.debug("Exception when processing message at {}", position, ex);
            return true;
        }
    }

    @Override
    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }

    @Override
    public String toString() {
        return "ReorderingFlowProcessor{" + handler + '}';
    }
}
