package exchange.core2.revelator.processors.pipelined;

import exchange.core2.revelator.*;
import exchange.core2.revelator.fences.*;
import exchange.core2.revelator.processors.IFlowProcessor;

import java.util.*;
import java.util.function.Supplier;

/**
 * Single-owner cooperative pipeline. Stages preserve input order; false suspends the current stage.
 * Sessions are reused after the final stage. A stage must not repeat observable effects on retry.
 */
public final class PipelinedFlowProcessor<S extends PipelinedFlowSession>
        implements IFlowProcessor {
    private static final int WINDOW = 64;
    private final List<PipelinedStageHandler<S>> handlers;
    private final List<S> sessions = new ArrayList<>(WINDOW);
    private final IFence input;
    private final SingleWriterFence release = new SingleWriterFence();
    private final int mask;
    private final long[] buffer;
    private final FrameLayout layout;

    public PipelinedFlowProcessor(
            List<PipelinedStageHandler<S>> handlers,
            Supplier<S> factory,
            IFence input,
            int mask,
            long[] buffer) {
        this(handlers, factory, input, mask, buffer, FrameLayout.packed());
    }

    public PipelinedFlowProcessor(
            List<PipelinedStageHandler<S>> handlers,
            Supplier<S> factory,
            IFence input,
            int mask,
            long[] buffer,
            FrameLayout layout) {
        if (handlers.isEmpty())
            throw new IllegalArgumentException("pipeline must have at least one stage");
        this.handlers = List.copyOf(handlers);
        this.input = Objects.requireNonNull(input);
        this.mask = mask;
        this.buffer = buffer;
        this.layout = layout;
        for (int i = 0; i < WINDOW; i++) sessions.add(Objects.requireNonNull(factory.get()));
    }

    @Override
    public void run() {
        long nextFrame = 0, available = 0, admitted = 0, completed = 0;
        long[] stageNext = new long[handlers.size()];
        boolean stopping = false;
        long poisonEnd = 0;
        while (!Thread.currentThread().isInterrupted()) {
            boolean work = false;
            if (!stopping && admitted - completed < WINDOW) {
                if (nextFrame >= available) available = input.getAcquire(nextFrame);
                while (nextFrame < available && admitted - completed < WINDOW) {
                    int index = (int) (nextFrame & mask);
                    if (buffer.length - index < Revelator.MSG_HEADER_SIZE || buffer[index] == 0) {
                        nextFrame = layout.nextLap(nextFrame, mask);
                        continue;
                    }
                    long header = buffer[index];
                    byte type = (byte) ((header >>> 56) & 31);
                    int size = (int) buffer[index + 2];
                    if (size < 0 || size > buffer.length - index - Revelator.MSG_HEADER_SIZE)
                        throw new IllegalStateException("invalid frame at " + nextFrame);
                    long end = nextFrame + Revelator.MSG_HEADER_SIZE + size;
                    if (type == Revelator.MSG_TYPE_POISON_PILL) {
                        stopping = true;
                        poisonEnd = end;
                        break;
                    }
                    S session = sessions.get((int) (admitted & (WINDOW - 1)));
                    session.frameStart = nextFrame;
                    session.endExclusive = end;
                    session.bufferIndex = index + Revelator.MSG_HEADER_SIZE;
                    session.payloadSize = size;
                    session.messageType = type;
                    session.timestamp = buffer[index + 1];
                    session.correlationId = header & 0x00FF_FFFF_FFFF_FFFFL;
                    session.userCookie = 0;
                    session.reset();
                    admitted++;
                    nextFrame = layout.nextFrame(end);
                    work = true;
                }
            }
            for (int stage = 0; stage < handlers.size(); stage++) {
                long ready = stage == 0 ? admitted : stageNext[stage - 1];
                if (stageNext[stage] < ready) {
                    S session = sessions.get((int) (stageNext[stage] & (WINDOW - 1)));
                    if (handlers.get(stage).process(session)) {
                        stageNext[stage]++;
                        work = true;
                        if (stage == handlers.size() - 1) {
                            completed = stageNext[stage];
                            release.setRelease(layout.nextFrame(session.endExclusive));
                        }
                    }
                }
            }
            if (stopping && completed == admitted) {
                for (var handler : handlers) handler.onShutdown();
                release.setRelease(poisonEnd);
                return;
            }
            if (!work) Thread.onSpinWait();
        }
    }

    @Override
    public SingleWriterFence getReleasingFence() {
        return release;
    }
}
