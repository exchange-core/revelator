package exchange.core2.revelator;

import exchange.core2.revelator.fences.HeaderCommitFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import org.agrona.BitUtil;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Single-producer ring. Positions and sizes are in longs. One thread claims, writes and publishes; every claim is
 * filled completely before the next one. Handlers borrow the payload until they return. Stop the producer and
 * publish its last batch before stopAsync/close. See docs/API.md for the full contract.
 */
public final class Revelator implements AutoCloseable {

    public static final int MSG_HEADER_SIZE = 4;
    public static final byte MSG_TYPE_POISON_PILL = 31;

    /**
     * How readers learn that a message is complete.
     */
    public enum PublicationMode {
        /** publish() moves a shared cursor (one line every reader polls) */
        CURSOR,
        /** Experimental: publish() writes the commit word of each message, readers scan commit words (see HeaderCommitFence) */
        HEADER_COMMIT
    }

    public enum State {
        NEW,
        RUNNING,
        STOPPING,
        TERMINATED,
        FAILED
    }

    private final int bufferSize;
    private final int indexMask;
    private final long[] buffer;

    private final List<? extends IFlowProcessor> processors;
    private final ThreadFactory threadFactory;
    private final List<Thread> threads = new CopyOnWriteArrayList<>();

    private final SingleWriterFence inboundFence; // single publisher
    private final HeaderCommitFence commitFence;  // HEADER_COMMIT only
    private final IFence releasingFence;

    private final PublicationMode publicationMode;
    private final FrameLayout frameLayout;

    // producer state, single writer
    private long reservedPosition = 0L;       // nextValue = single writer sequencer position in Disruptor
    private long cachedOutboundPosition = 0L; // cachedValue = min gating sequence in Disruptor
    private long lastClaimEnd = 0L;           // end of the last claimed message
    private long publishedPosition = 0L;      // end of the last published message
    private long tailStrike = 0L;

    // HEADER_COMMIT: claimed, not yet published messages - start, header1, timestamp, size (header1 0: header written)
    private static final int PENDING_MAX = 1024; // claims without publish beyond this publish the oldest
    private final long[] pending = new long[PENDING_MAX * 4];
    private long pendingHead = 0L;
    private long pendingTail = 0L;

    // lifecycle
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private volatile State state = State.NEW;
    private volatile Throwable failure;
    private volatile long stopDeadline = Long.MAX_VALUE;

    public static Revelator create(final int capacityLongs,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory) {
        return create(capacityLongs, flowProcessorsFactory, threadFactory, PublicationMode.CURSOR);
    }

    public static Revelator create(final int capacityLongs,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory,
                                   final PublicationMode publicationMode) {
        return create(capacityLongs, flowProcessorsFactory, threadFactory, publicationMode, FrameLayout.packed());
    }

    public static Revelator create(final int capacityLongs,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory,
                                   final PublicationMode publicationMode,
                                   final FrameLayout frameLayout) {
        Objects.requireNonNull(flowProcessorsFactory);
        Objects.requireNonNull(threadFactory);
        Objects.requireNonNull(publicationMode);
        Objects.requireNonNull(frameLayout);
        if (capacityLongs < 1024 || !BitUtil.isPowerOfTwo(capacityLongs)) {
            throw new IllegalArgumentException("capacityLongs must be a power of two >= 1024");
        }

        final SingleWriterFence inboundFence = new SingleWriterFence();
        final int indexMask = capacityLongs - 1;
        final long[] buffer = new long[capacityLongs];

        // first-level processors read either the cursor or the commit words, the rest of the chain is the same
        final HeaderCommitFence commitFence = publicationMode == PublicationMode.HEADER_COMMIT
                ? new HeaderCommitFence(buffer, indexMask, frameLayout)
                : null;
        final IFence readersFence = commitFence != null ? commitFence : inboundFence;

        final IFlowProcessorsFactory.ProcessorsChain chain = Objects.requireNonNull(
                flowProcessorsFactory.createProcessors(
                        readersFence,
                        new RevelatorConfig(indexMask, capacityLongs, buffer, frameLayout)));
        if (chain.getProcessors().isEmpty()) {
            throw new IllegalArgumentException("processor graph must not be empty");
        }

        return new Revelator(buffer, chain, threadFactory, inboundFence, commitFence, publicationMode, frameLayout);
    }

    private Revelator(final long[] buffer,
                      final IFlowProcessorsFactory.ProcessorsChain chain,
                      final ThreadFactory threadFactory,
                      final SingleWriterFence inboundFence,
                      final HeaderCommitFence commitFence,
                      final PublicationMode publicationMode,
                      final FrameLayout frameLayout) {
        this.buffer = buffer;
        this.bufferSize = buffer.length;
        this.indexMask = buffer.length - 1;
        this.processors = List.copyOf(chain.getProcessors());
        this.threadFactory = threadFactory;
        this.inboundFence = inboundFence;
        this.commitFence = commitFence;
        this.releasingFence = Objects.requireNonNull(chain.getReleasingFence());
        this.publicationMode = publicationMode;
        this.frameLayout = frameLayout;
    }

    public synchronized void start() {
        if (state != State.NEW) {
            throw new IllegalStateException("start requires NEW, was " + state);
        }
        state = State.RUNNING;
        try {
            for (final IFlowProcessor processor : processors) {
                final Thread thread = Objects.requireNonNull(
                        threadFactory.newThread(() -> runProcessor(processor)), "threadFactory returned null");
                if (thread.getName().startsWith("Thread-")) {
                    thread.setName("PROC-" + threads.size()); // keep a name the factory has chosen
                }
                threads.add(thread);
                thread.start();
            }
        } catch (final Throwable error) {
            fail(error);
            throw new IllegalStateException("processor startup failed", error);
        }
    }

    // a processor returns on the shutdown message; returning earlier or throwing fails the whole ring
    private void runProcessor(final IFlowProcessor processor) {
        try {
            processor.run();
            if (state == State.RUNNING) {
                fail(new IllegalStateException("processor exited before shutdown: " + processor));
            }
        } catch (final Throwable error) {
            if (state != State.FAILED) {
                fail(error);
            }
        }
    }

    public State state() {
        return state;
    }

    public Throwable failure() {
        return failure;
    }

    // stops everything: workers are interrupted, the producer and stopAsync callers get the error
    private synchronized void fail(final Throwable error) {
        if (state == State.FAILED || state == State.TERMINATED) {
            return;
        }
        failure = error;
        state = State.FAILED;
        for (final Thread thread : threads) {
            if (thread != Thread.currentThread()) {
                thread.interrupt();
            }
        }
        termination.completeExceptionally(error);
    }

    private void requireRunning() {
        if (state != State.RUNNING) {
            throw new IllegalStateException("publisher requires RUNNING, was " + state, failure);
        }
    }

    /**
     * Claims space for one message and writes its header (with aligned header-commit frames the header is written
     * at publish, together with the commit word).
     *
     * @return position of the payload
     */
    public long claimSingleMessage(final int payloadLongs,
                                   final long timestamp,
                                   final long correlationId,
                                   final byte messageType) {
        requireRunning();
        if (payloadLongs < 0 || payloadLongs > bufferSize - 8 - MSG_HEADER_SIZE
                || messageType < 1 || messageType >= MSG_TYPE_POISON_PILL || (correlationId >>> 56) != 0) {
            throw invalidClaim(payloadLongs, correlationId, messageType);
        }
        return claim(payloadLongs, timestamp, correlationId, messageType);
    }

    private static IllegalArgumentException invalidClaim(int payloadLongs, long correlationId, byte messageType) {
        return new IllegalArgumentException("invalid claim: messageType=" + messageType + " (1..30), correlationId="
                + correlationId + " (0..2^56-1), payloadLongs=" + payloadLongs);
    }

    // The common path stays small so that it inlines into the publisher's loop; padding and waiting are out of line.
    private long claim(final int payloadLongs, final long timestamp, final long correlationId, final byte messageType) {
        final int fullMessageSize = payloadLongs + MSG_HEADER_SIZE;
        long msgStart = reservedPosition;
        int index = (int) (msgStart & indexMask);
        if (bufferSize - index < fullMessageSize) {
            msgStart = padToNextLap(msgStart, index);
            index = (int) (msgStart & indexMask);
        }
        if (publicationMode == PublicationMode.HEADER_COMMIT) {
            final long msgEnd = msgStart + fullMessageSize;
            claimCommitted(msgStart, msgEnd, index, (((long) messageType) << 56) | correlationId, timestamp, payloadLongs);
            lastClaimEnd = msgEnd;
            reservedPosition = frameLayout.nextFrame(msgEnd);
        } else {
            // Nothing derived is computed before the wait: values live across that call are kept on the stack, and C2
            // stored them there on the common path too (11 extra stores per message made the publisher store-buffer
            // bound with readers on its heels, MULTICAST3 20 -> 9 MT/s).
            final long wrapPoint = msgStart + fullMessageSize - bufferSize;
            if (wrapPoint > cachedOutboundPosition) {
                awaitSpace(msgStart, wrapPoint);
            }
            buffer[index] = (((long) messageType) << 56) | correlationId;
            buffer[index + 1] = timestamp;
            buffer[index + 2] = payloadLongs;
            // buffer[index + 3] is the commit word in HEADER_COMMIT mode, unused with CURSOR
            final long msgEnd = msgStart + fullMessageSize;
            lastClaimEnd = msgEnd;
            reservedPosition = frameLayout.nextFrame(msgEnd);
        }
        return msgStart + MSG_HEADER_SIZE;
    }

    private void claimCommitted(final long msgStart, final long msgEnd, final int index,
                                final long header1, final long timestamp, final int payloadLongs) {
        final long wrapPoint = msgEnd - bufferSize;
        if (wrapPoint > cachedOutboundPosition) {
            awaitSpace(msgStart, wrapPoint);
        }
        if (frameLayout.isAligned()) {
            // the header line is the one readers spin on: header and commit word go out together at publish
            addPending(msgStart, header1, timestamp, payloadLongs);
        } else {
            buffer[index] = header1;
            buffer[index + 1] = timestamp;
            buffer[index + 2] = payloadLongs;
            addPending(msgStart, 0L, 0L, payloadLongs);
        }
    }

    // The message does not fit before the end of the ring: pad the rest of the lap, continue from the next lap.
    // Padding is published right away: a large message at the next lap can wait for space that readers free only
    // after they have passed the padding.
    private long padToNextLap(final long msgStart, final int index) {
        flushComplete(lastClaimEnd);
        final long nextLapStart = frameLayout.nextLap(msgStart, indexMask);
        final long wrapPoint = nextLapStart - bufferSize;
        if (wrapPoint > cachedOutboundPosition) {
            awaitSpace(msgStart, wrapPoint);
        }
        buffer[index] = 0L;
        if (publicationMode == PublicationMode.HEADER_COMMIT) {
            if (bufferSize - index >= MSG_HEADER_SIZE) {
                // padding has no payload: commit it now, readers cannot reach it before the messages ahead of it
                commitFence.commit(index, msgStart);
            }
        } else {
            inboundFence.setRelease(nextLapStart);
        }
        reservedPosition = nextLapStart;
        return nextLapStart;
    }

    // HEADER_COMMIT: claimed, not yet published messages - start, header1, timestamp, size (header1 0: header written)
    private void addPending(final long msgStart, final long header1, final long timestamp, final long payloadLongs) {
        if (pendingTail - pendingHead == PENDING_MAX) {
            commitUpTo(msgStart);
        }
        final int p = (int) (pendingTail++ & (PENDING_MAX - 1)) * 4;
        pending[p] = msgStart;
        pending[p + 1] = header1;
        pending[p + 2] = timestamp;
        pending[p + 3] = payloadLongs;
    }

    // Commits the pending messages that start before end, last one first. A reader that sees the commit word of the
    // first message also sees everything written before it - the other commit words of the batch - and goes through
    // the batch without waiting; in forward order it would chase the publisher message by message.
    private void commitUpTo(final long end) {
        long upTo = pendingHead;
        while (upTo != pendingTail && pending[(int) (upTo & (PENDING_MAX - 1)) * 4] < end) {
            upTo++;
        }
        if (upTo == pendingHead) {
            return;
        }
        final int last = (int) ((upTo - 1) & (PENDING_MAX - 1)) * 4;
        final long batchEnd = pending[last] + MSG_HEADER_SIZE + pending[last + 3];
        for (long i = upTo - 1; i >= pendingHead; i--) {
            final int p = (int) (i & (PENDING_MAX - 1)) * 4;
            final long start = pending[p];
            final int index = (int) (start & indexMask);
            if (pending[p + 1] != 0L) {
                buffer[index] = pending[p + 1];
                buffer[index + 1] = pending[p + 2];
                buffer[index + 2] = pending[p + 3];
            }
            commitFence.commit(index, start);
        }
        pendingHead = upTo;
        publishedPosition = batchEnd;
    }

    private void flushComplete(final long end) {
        if (publicationMode == PublicationMode.HEADER_COMMIT) {
            commitUpTo(end);
        } else if (end > publishedPosition) {
            inboundFence.setRelease(end);
            publishedPosition = end;
        }
    }

    private void awaitSpace(final long msgStart, final long wrapPoint) {
        // make everything claimed so far visible (claims are complete by contract): readers must free space for us
        flushComplete(lastClaimEnd);
        long minSequence;
        while (wrapPoint > (minSequence = Math.min(releasingFence.getAcquire(cachedOutboundPosition), msgStart))) {
            if (state == State.FAILED) {
                throw new IllegalStateException("consumer failed", failure);
            }
            if (Thread.currentThread().isInterrupted() || System.nanoTime() > stopDeadline) {
                throw new CancellationException("ring admission cancelled");
            }
            // parkNanos(1) sleeps ~50us (timer slack) and collapsed saturated throughput; spin instead
            Thread.onSpinWait();
            tailStrike++;
        }
        cachedOutboundPosition = minSequence;
    }

    /**
     * Makes the claimed messages up to endExclusive visible to readers. Several messages can be published by one
     * call (batching): pass the end of the last one, or of an earlier one to publish a prefix.
     */
    public void publish(final long endExclusive) {
        requireRunning();
        if (endExclusive != lastClaimEnd) {
            validatePrefix(endExclusive);
        }
        flushComplete(endExclusive);
    }

    // cold path: end must be the end of a claimed message that is not published yet
    private void validatePrefix(final long end) {
        if (end == publishedPosition) {
            return;
        }
        if (end < publishedPosition || end > lastClaimEnd) {
            throw new IllegalArgumentException("invalid publication prefix: " + end + " (published "
                    + publishedPosition + ", claimed " + lastClaimEnd + ")");
        }
        if (publicationMode == PublicationMode.HEADER_COMMIT) {
            for (long i = pendingHead; i < pendingTail; i++) {
                final int p = (int) (i & (PENDING_MAX - 1)) * 4;
                if (pending[p] + MSG_HEADER_SIZE + pending[p + 3] == end) {
                    return;
                }
            }
        } else {
            long pos = publishedPosition == 0 ? 0 : frameLayout.nextFrame(publishedPosition);
            while (pos < end) {
                final int index = (int) (pos & indexMask);
                if (bufferSize - index < MSG_HEADER_SIZE || buffer[index] == 0L) {
                    pos = frameLayout.nextLap(pos, indexMask);
                    continue;
                }
                final long frameEnd = pos + MSG_HEADER_SIZE + buffer[index + 2];
                if (frameEnd == end) {
                    return;
                }
                if (frameEnd <= pos || frameEnd > end) {
                    break;
                }
                pos = frameLayout.nextFrame(frameEnd);
            }
        }
        throw new IllegalArgumentException("publish requires the end of a claimed message: " + end);
    }

    public void writeLongData(final long payloadPosition, final int offset, final long value) {
        buffer[((int) payloadPosition & indexMask) + offset] = value;
    }

    public void writeLongData(final long payloadPosition, final int offset,
                              final long value0, final long value1, final long value2,
                              final long value3, final long value4, final long value5) {
        final int pos = ((int) payloadPosition & indexMask) + offset;
        buffer[pos] = value0;
        buffer[pos + 1] = value1;
        buffer[pos + 2] = value2;
        buffer[pos + 3] = value3;
        buffer[pos + 4] = value4;
        buffer[pos + 5] = value5;
    }

    /** Raw ring access for trusted adapters; callers are responsible for ownership and bounds. */
    public void writeLongDataUnsafe(final int index, final long value) {
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
     * Sends the shutdown message after everything published and waits for all processors to finish, on a separate
     * thread: returns at once, also when the ring is full. The producer must be stopped, with its last batch
     * published. The result fails if the processors do not finish within the timeout or one of them fails.
     */
    public synchronized CompletableFuture<Void> stopAsync(final Duration timeout) {
        final long timeoutNs = Objects.requireNonNull(timeout).toNanos();
        if (timeoutNs <= 0) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (state == State.NEW) {
            state = State.TERMINATED;
            termination.complete(null);
            return termination.copy();
        }
        if (state != State.RUNNING) {
            return termination.copy();
        }
        if (lastClaimEnd != publishedPosition) {
            throw new IllegalStateException("publish outstanding claims before shutdown");
        }
        state = State.STOPPING;
        stopDeadline = System.nanoTime() + timeoutNs;
        final Thread coordinator = new Thread(this::shutdown, "revelator-shutdown");
        coordinator.setDaemon(true);
        coordinator.start();
        return termination.copy();
    }

    // runs on the coordinator thread: the producer is stopped, so this thread becomes the producer for the poison pill
    private void shutdown() {
        try {
            flushComplete(claim(0, 0L, 0L, MSG_TYPE_POISON_PILL));
            for (final Thread thread : threads) {
                while (thread.isAlive()) {
                    final long remainingNs = stopDeadline - System.nanoTime();
                    if (remainingNs <= 0) {
                        throw new TimeoutException("processors did not finish before the shutdown deadline");
                    }
                    thread.join(Math.clamp(TimeUnit.NANOSECONDS.toMillis(remainingNs), 1L, 100L));
                }
            }
            synchronized (this) {
                if (state != State.FAILED) {
                    state = State.TERMINATED;
                    termination.complete(null);
                }
            }
        } catch (final Throwable error) {
            fail(error);
        }
    }

    /**
     * Interrupts workers; handlers must cooperate with interruption. Does not roll back applied effects.
     */
    public void abort(final Throwable reason) {
        fail(Objects.requireNonNull(reason));
    }

    /**
     * Graceful stop: waits up to 30 seconds for the processors to handle everything published.
     */
    @Override
    public void close() {
        if (threads.contains(Thread.currentThread())) {
            throw new IllegalStateException("close cannot join its own processor thread");
        }
        try {
            stopAsync().get(31, TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            abort(ex);
            throw new IllegalStateException("shutdown interrupted", ex);
        } catch (final ExecutionException | TimeoutException ex) {
            abort(ex);
            throw new IllegalStateException("shutdown failed", ex);
        }
    }
}
