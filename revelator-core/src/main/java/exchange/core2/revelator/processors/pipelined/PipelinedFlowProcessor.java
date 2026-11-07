package exchange.core2.revelator.processors.pipelined;

import exchange.core2.revelator.FrameLayout;
import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Runs several stages on one thread. Up to WINDOW messages are in flight; every stage takes them in log order and
 * may return false to try the same message again later (a dependency is not ready) while the stages before it go on.
 * Sessions are reused once the last stage is done with them. A stage must not repeat observable effects on retry.
 */
public final class PipelinedFlowProcessor<S extends PipelinedFlowSession> implements IFlowProcessor {

    private static final int WINDOW = 64; // 2^N

    // progress is published when the processor has caught up with the producer, otherwise at least this often
    // (longs): publishing after every message made the producer, spinning on a full ring, pull the line away from
    // this thread once per message (PIPELINE3_1T burst 26 -> 12 M msg/s)
    private static final int RELEASE_EVERY = Integer.getInteger("revelator.releaseEvery", 4096);

    private final PipelinedStageHandler<S>[] handlers;
    private final S[] sessions;
    private final IFence inboundFence;
    private final SingleWriterFence releasingFence = new SingleWriterFence();
    private final int indexMask;
    private final long[] buffer;
    private final FrameLayout frameLayout;

    public PipelinedFlowProcessor(final List<PipelinedStageHandler<S>> handlers,
                                  final Supplier<S> sessionsFactory,
                                  final IFence inboundFence,
                                  final int indexMask,
                                  final long[] buffer) {
        this(handlers, sessionsFactory, inboundFence, indexMask, buffer, FrameLayout.packed());
    }

    @SuppressWarnings("unchecked")
    public PipelinedFlowProcessor(final List<PipelinedStageHandler<S>> handlers,
                                  final Supplier<S> sessionsFactory,
                                  final IFence inboundFence,
                                  final int indexMask,
                                  final long[] buffer,
                                  final FrameLayout frameLayout) {
        if (handlers.isEmpty()) {
            throw new IllegalArgumentException("pipeline must have at least one stage");
        }
        this.handlers = handlers.toArray(PipelinedStageHandler[]::new);
        this.sessions = (S[]) new PipelinedFlowSession[WINDOW];
        for (int i = 0; i < WINDOW; i++) {
            sessions[i] = Objects.requireNonNull(sessionsFactory.get());
        }
        this.inboundFence = Objects.requireNonNull(inboundFence);
        this.indexMask = indexMask;
        this.buffer = buffer;
        this.frameLayout = frameLayout;
    }

    @Override
    public void run() {

        final PipelinedStageHandler<S>[] handlers = this.handlers;
        final int lastStage = handlers.length - 1;
        final long[] stageNext = new long[handlers.length]; // next message number for each stage

        long nextFrame = 0L;     // next frame to admit
        long available = 0L;     // known end of published messages
        long admitted = 0L;      // messages admitted so far
        long completed = 0L;     // messages done by the last stage
        long completedEnd = 0L;  // frame after the last completed message
        long released = 0L;
        long poisonEnd = -1L;    // >= 0 once the shutdown message is seen; it is not admitted

        while (!Thread.currentThread().isInterrupted()) {

            boolean work = false;

            if (poisonEnd < 0 && admitted - completed < WINDOW) {
                if (nextFrame >= available) {
                    available = inboundFence.getAcquire(nextFrame);
                }
                while (nextFrame < available && admitted - completed < WINDOW) {
                    final int index = (int) (nextFrame & indexMask);
                    if (buffer.length - index < Revelator.MSG_HEADER_SIZE || buffer[index] == 0L) {
                        nextFrame = frameLayout.nextLap(nextFrame, indexMask); // padding
                        continue;
                    }
                    final long header1 = buffer[index];
                    final byte msgType = (byte) ((int) (header1 >>> 56) & 0x1F);
                    final int payloadSize = (int) buffer[index + 2];
                    if (payloadSize < 0 || payloadSize > buffer.length - index - Revelator.MSG_HEADER_SIZE) {
                        throw new IllegalStateException("invalid frame at " + nextFrame + ": payloadSize=" + payloadSize);
                    }
                    final long end = nextFrame + Revelator.MSG_HEADER_SIZE + payloadSize;
                    if (msgType == Revelator.MSG_TYPE_POISON_PILL) {
                        poisonEnd = end;
                        break;
                    }
                    final S session = sessions[(int) admitted & (WINDOW - 1)];
                    session.frameStart = nextFrame;
                    session.endExclusive = end;
                    session.bufferIndex = index + Revelator.MSG_HEADER_SIZE;
                    session.payloadSize = payloadSize;
                    session.messageType = msgType;
                    session.timestamp = buffer[index + 1];
                    session.correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;
                    session.userCookie = 0;
                    session.reset();
                    admitted++;
                    nextFrame = frameLayout.nextFrame(end);
                    work = true;
                }
            }

            // one step of every stage, in stage order, so a message can pass all stages in one turn
            long ready = admitted;
            for (int stage = 0; stage <= lastStage; stage++) {
                final long seq = stageNext[stage];
                if (seq < ready) {
                    final S session = sessions[(int) seq & (WINDOW - 1)];
                    if (handlers[stage].process(session)) {
                        stageNext[stage] = seq + 1;
                        work = true;
                        if (stage == lastStage) {
                            completed = seq + 1;
                            completedEnd = frameLayout.nextFrame(session.endExclusive);
                        }
                    }
                }
                ready = stageNext[stage];
            }

            if (completedEnd != released
                    && (nextFrame >= available || completedEnd - released >= RELEASE_EVERY)) {
                releasingFence.setRelease(completedEnd);
                released = completedEnd;
            }

            if (poisonEnd >= 0 && completed == admitted) {
                for (final PipelinedStageHandler<S> handler : handlers) {
                    handler.onShutdown();
                }
                releasingFence.setRelease(poisonEnd);
                return;
            }

            if (!work) {
                Thread.onSpinWait();
            }
        }
    }

    @Override
    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }
}
