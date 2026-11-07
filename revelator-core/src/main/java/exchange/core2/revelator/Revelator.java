package exchange.core2.revelator;

import exchange.core2.revelator.fences.*;
import exchange.core2.revelator.processors.*;

import org.agrona.BitUtil;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Single-producer runtime. Positions and sizes are measured in long words. Fully fill each claim
 * before claiming another. Publish the end of the last payload. Quiesce the producer and publish
 * its outstanding batch before calling stopAsync/close. Consumers borrow the payload until their
 * handler returns; retaining it is forbidden.
 */
public final class Revelator implements AutoCloseable {
    public static final int MSG_HEADER_SIZE = 4;
    public static final byte MSG_TYPE_POISON_PILL = 31;

    /** Legacy application control type; the runtime no longer interprets its payload. */
    public static final byte MSG_TYPE_TEST_CONTROL = 30;

    public enum PublicationMode {
        CURSOR,
        /** Disabled: unsafe for variable-sized frames. */
        HEADER_COMMIT
    }

    public enum State {
        NEW,
        RUNNING,
        STOPPING,
        TERMINATED,
        FAILED
    }

    private final int bufferSize, indexMask;
    private final long[] buffer;
    private final List<? extends IFlowProcessor> processors;
    private final ThreadFactory threadFactory;
    private final SingleWriterFence inboundFence;
    private final IFence releasingFence;
    private final FrameLayout frameLayout;
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private volatile State state = State.NEW;
    private volatile Throwable failure;
    private long reservedPosition,
            cachedOutboundPosition,
            lastClaimEnd,
            publishedPosition,
            tailStrike;
    private volatile long stopDeadline = Long.MAX_VALUE;

    public static Revelator create(
            int capacityLongs, IFlowProcessorsFactory factory, ThreadFactory threads) {
        return create(
                capacityLongs, factory, threads, PublicationMode.CURSOR, FrameLayout.packed());
    }

    public static Revelator create(
            int capacityLongs,
            IFlowProcessorsFactory factory,
            ThreadFactory threads,
            PublicationMode mode) {
        return create(capacityLongs, factory, threads, mode, FrameLayout.packed());
    }

    public static Revelator create(
            int capacityLongs,
            IFlowProcessorsFactory factory,
            ThreadFactory threads,
            PublicationMode mode,
            FrameLayout layout) {
        Objects.requireNonNull(factory);
        Objects.requireNonNull(threads);
        Objects.requireNonNull(layout);
        Objects.requireNonNull(mode);
        if (mode != PublicationMode.CURSOR)
            throw new UnsupportedOperationException(
                    "HEADER_COMMIT is disabled: variable-frame metadata can alias old payload");
        if (capacityLongs < 1024 || !BitUtil.isPowerOfTwo(capacityLongs))
            throw new IllegalArgumentException("capacityLongs must be a power of two >= 1024");
        SingleWriterFence input = new SingleWriterFence();
        long[] data = new long[capacityLongs];
        var chain =
                Objects.requireNonNull(
                        factory.createProcessors(
                                input,
                                new RevelatorConfig(
                                        capacityLongs - 1, capacityLongs, data, layout)));
        if (chain.getProcessors().isEmpty())
            throw new IllegalArgumentException("processor graph must not be empty");
        return new Revelator(data, chain, threads, input, layout);
    }

    private Revelator(
            long[] data,
            IFlowProcessorsFactory.ProcessorsChain chain,
            ThreadFactory tf,
            SingleWriterFence input,
            FrameLayout layout) {
        buffer = data;
        bufferSize = data.length;
        indexMask = data.length - 1;
        processors = List.copyOf(chain.getProcessors());
        threadFactory = tf;
        inboundFence = input;
        releasingFence = Objects.requireNonNull(chain.getReleasingFence());
        frameLayout = layout;
    }

    public synchronized void start() {
        if (state != State.NEW) throw new IllegalStateException("start requires NEW, was " + state);
        state = State.RUNNING;
        try {
            for (IFlowProcessor processor : processors) {
                Thread t =
                        Objects.requireNonNull(
                                threadFactory.newThread(
                                        () -> {
                                            try {
                                                processor.run();
                                                if (state == State.RUNNING)
                                                    fail(
                                                            new IllegalStateException(
                                                                    "processor exited before"
                                                                            + " shutdown"));
                                            } catch (Throwable error) {
                                                if (state != State.FAILED) fail(error);
                                            }
                                        }),
                                "threadFactory returned null");
                threads.add(t);
                t.start();
            }
        } catch (Throwable error) {
            fail(error);
            throw new IllegalStateException("processor startup failed", error);
        }
    }

    public State state() {
        return state;
    }

    public Throwable failure() {
        return failure;
    }

    private synchronized void fail(Throwable error) {
        if (state == State.FAILED || state == State.TERMINATED) return;
        failure = error;
        state = State.FAILED;
        for (Thread t : threads) if (t != Thread.currentThread()) t.interrupt();
        termination.completeExceptionally(error);
    }

    private void requireRunning() {
        if (state != State.RUNNING)
            throw new IllegalStateException("publisher requires RUNNING, was " + state, failure);
    }

    public long claimSingleMessage(
            int payloadLongs, long timestamp, long correlationId, byte type) {
        requireRunning();
        if (payloadLongs < 0
                || payloadLongs > bufferSize - 8 - MSG_HEADER_SIZE
                || type < 1
                || type >= MSG_TYPE_POISON_PILL
                || (correlationId >>> 56) != 0)
            throw new IllegalArgumentException(
                    "invalid payloadLongs, messageType (1..30), or 56-bit correlationId");
        return claim(payloadLongs, timestamp, correlationId, type);
    }

    private long claim(int payloadLongs, long timestamp, long id, byte type) {
        int full = payloadLongs + MSG_HEADER_SIZE;
        long start = reservedPosition;
        int index = (int) (start & indexMask);
        if (bufferSize - index < full) {
            long next = frameLayout.nextLap(start, indexMask);
            awaitSpace(start, next + full - bufferSize);
            buffer[index] = 0;
            start = next;
            index = (int) (start & indexMask);
        } else if (start + full - bufferSize > cachedOutboundPosition)
            awaitSpace(start, start + full - bufferSize);
        lastClaimEnd = start + full;
        reservedPosition = frameLayout.nextFrame(lastClaimEnd);
        buffer[index] = ((long) type << 56) | id;
        buffer[index + 1] = timestamp;
        buffer[index + 2] = payloadLongs;
        return start + MSG_HEADER_SIZE;
    }

    private void awaitSpace(long start, long wrap) {
        // Each previous claim must already be fully written. Flush it before waiting for consumers.
        inboundFence.setVolatile(start);
        publishedPosition = lastClaimEnd;
        long min;
        while (wrap > (min = Math.min(releasingFence.getAcquire(cachedOutboundPosition), start))) {
            if (state == State.FAILED) throw new IllegalStateException("consumer failed", failure);
            if (Thread.currentThread().isInterrupted() || System.nanoTime() > stopDeadline)
                throw new CancellationException("ring admission cancelled");
            tailStrike++;
            Thread.onSpinWait();
        }
        cachedOutboundPosition = min;
    }

    /**
     * Publishes the complete outstanding batch. The argument must be its last payload end
     * (exclusive).
     */
    public void publish(long endExclusive) {
        requireRunning();
        if (endExclusive != lastClaimEnd || endExclusive < publishedPosition)
            throw new IllegalArgumentException(
                    "publish requires the end of the last claimed payload");
        inboundFence.setRelease(endExclusive);
        publishedPosition = endExclusive;
    }

    public void writeLongData(long payloadPosition, int offset, long value) {
        buffer[((int) payloadPosition & indexMask) + offset] = value;
    }

    public void writeLongData(
            long payloadPosition, int offset, long a, long b, long c, long d, long e, long f) {
        int p = ((int) payloadPosition & indexMask) + offset;
        buffer[p] = a;
        buffer[p + 1] = b;
        buffer[p + 2] = c;
        buffer[p + 3] = d;
        buffer[p + 4] = e;
        buffer[p + 5] = f;
    }

    /** Raw ring access for trusted adapters; callers are responsible for ownership and bounds. */
    public void writeLongDataUnsafe(int index, long value) {
        buffer[index] = value;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getIndexMask() {
        return indexMask;
    }

    public long getTailStrike() {
        return tailStrike;
    }

    public CompletableFuture<Void> stopAsync() {
        return stopAsync(Duration.ofSeconds(30));
    }

    /**
     * The producer must be quiescent with its last batch published. Always returns without waiting
     * for ring space.
     */
    public synchronized CompletableFuture<Void> stopAsync(Duration timeout) {
        Objects.requireNonNull(timeout);
        long nanos = timeout.toNanos();
        if (nanos <= 0) throw new IllegalArgumentException("timeout must be positive");
        if (state == State.NEW) {
            state = State.TERMINATED;
            termination.complete(null);
            return termination.copy();
        }
        if (state != State.RUNNING) return termination.copy();
        if (lastClaimEnd != publishedPosition)
            throw new IllegalStateException("publish outstanding claims before shutdown");
        state = State.STOPPING;
        stopDeadline = System.nanoTime() + nanos;
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                long end = claim(0, 0, 0, MSG_TYPE_POISON_PILL);
                                inboundFence.setRelease(end);
                                for (Thread t : threads) {
                                    while (t.isAlive()) {
                                        long remaining = stopDeadline - System.nanoTime();
                                        if (remaining <= 0)
                                            throw new TimeoutException(
                                                    "processor graph did not drain before shutdown"
                                                            + " deadline");
                                        t.join(
                                                Math.max(
                                                        1,
                                                        Math.min(
                                                                100,
                                                                TimeUnit.NANOSECONDS.toMillis(
                                                                        remaining))));
                                    }
                                }
                                synchronized (this) {
                                    if (state != State.FAILED) {
                                        state = State.TERMINATED;
                                        termination.complete(null);
                                    }
                                }
                            } catch (Throwable error) {
                                fail(error);
                            }
                        },
                        "revelator-shutdown");
        coordinator.setDaemon(true);
        coordinator.start();
        return termination.copy();
    }

    /**
     * Interrupts workers; handlers must cooperate with interruption. Does not roll back applied
     * effects.
     */
    public void abort(Throwable reason) {
        fail(Objects.requireNonNull(reason));
    }

    @Override
    public void close() {
        if (threads.contains(Thread.currentThread()))
            throw new IllegalStateException("close cannot join its own processor thread");
        try {
            stopAsync().get(31, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abort(e);
            throw new IllegalStateException("shutdown interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            abort(e);
            throw new IllegalStateException("shutdown failed", e);
        }
    }
}
