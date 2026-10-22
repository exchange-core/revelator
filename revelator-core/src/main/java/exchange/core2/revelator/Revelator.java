package exchange.core2.revelator;

import exchange.core2.revelator.fences.HeaderCommitFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import org.agrona.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

public final class Revelator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Revelator.class);

    public static final int MSG_HEADER_SIZE = 4;
    public static final byte MSG_TYPE_POISON_PILL = 31;
    public static final byte MSG_TYPE_TEST_CONTROL = 30;

    private final int bufferSize;
    private final int indexMask;
    private final long[] buffer;

    private final List<? extends IFlowProcessor> processors;

    private final ThreadFactory threadFactory;

    private final SingleWriterFence inboundFence; // single publisher

    private final IFence releasingFence;

    private final PublicationMode publicationMode;

    private final List<Thread> threads = new ArrayList<>();

    private long reservedPosition = 0L; // nextValue = single writer sequencer position in Disruptor

    private long cachedOutboundPosition = 0L; // cachedValue = min gating sequence in Disruptor

    private long tailStrike = 0L;

    // HEADER_COMMIT: start positions of claimed, not yet published messages
    private static final int PENDING_MAX = 1024; // claims without publish beyond this publish the oldest
    private final long[] pendingStarts = new long[PENDING_MAX];
    private long pendingHead = 0L;
    private long pendingTail = 0L;

    /**
     * How readers learn that a message is complete.
     */
    public enum PublicationMode {
        /** publish() moves a shared cursor (one line every reader polls) */
        CURSOR,
        /** publish() writes the commit word of each message, readers scan commit words (see HeaderCommitFence) */
        HEADER_COMMIT
    }

    private CompletableFuture<Void> shutdownFuture;

    public static Revelator create(final int bufferSize,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory) {
        return create(bufferSize, flowProcessorsFactory, threadFactory, PublicationMode.CURSOR);
    }

    public static Revelator create(final int bufferSize,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory,
                                   final PublicationMode publicationMode) {

        if (!BitUtil.isPowerOfTwo(bufferSize)) {
            throw new IllegalArgumentException("Revelator buffer size must be 2^N");
        }

        if (bufferSize < 1024) {
            throw new IllegalArgumentException("Revelator buffer size must be > 1024 bytes");
        }


        final SingleWriterFence inboundFence = new SingleWriterFence(); // single publisher

        final int indexMask = bufferSize - 1;

        final long[] buffer = new long[bufferSize];

        // first-level processors read either the cursor or the commit words, the rest of the chain is the same
        final IFence readersFence = publicationMode == PublicationMode.HEADER_COMMIT
                ? new HeaderCommitFence(buffer, indexMask)
                : inboundFence;

        final IFlowProcessorsFactory.ProcessorsChain chain = flowProcessorsFactory.createProcessors(
                readersFence,
                new RevelatorConfig(indexMask, bufferSize, buffer));


        return new Revelator(
                bufferSize,
                indexMask,
                buffer,
                chain.getProcessors(),
                threadFactory,
                inboundFence,
                chain.getReleasingFence(),
                publicationMode);
    }


    private Revelator(final int bufferSize,
                      final int indexMask,
                      final long[] buffer,
                      final List<? extends IFlowProcessor> processors,
                      final ThreadFactory threadFactory,
                      final SingleWriterFence inboundFence,
                      final IFence outboundFence,
                      final PublicationMode publicationMode) {

        this.bufferSize = bufferSize;
        this.indexMask = indexMask;
        this.buffer = buffer;
        this.processors = processors;
        this.threadFactory = threadFactory;
        this.inboundFence = inboundFence;
        this.releasingFence = outboundFence;
        this.publicationMode = publicationMode;
    }

    public synchronized void start() {

        if (!threads.isEmpty()) {
            throw new IllegalStateException("already started");
        }

        int c = 0;

        for (final IFlowProcessor processor : processors) {

            final Thread thread = threadFactory.newThread(processor);
            final String threadName = "PROC-" + c; // TODO allow custom thread naming policy
            log.info("Starting processor {} (thread {})...", processor, threadName);
            thread.setName(threadName);
            thread.setDaemon(true);
            thread.start();
            threads.add(thread);
            c++;
        }
    }

    public synchronized CompletableFuture<Void> stopAsync() {

        if (shutdownFuture == null) {

            final long position = claimSingleMessage(0, 0L, 0L, MSG_TYPE_POISON_PILL);
            publish(position);

            shutdownFuture = CompletableFuture.runAsync(() -> {
                log.debug("Stopping {} revelator threads...", threads.size());
                for (final Thread thread : threads) {
                    try {
                        log.debug("Waiting revelator thread {} to stop ...", thread.getName());
                        thread.join();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                log.debug("All revelator threads stopped");
            });
        }

        return shutdownFuture;
    }

    /**
     * Claims space for one message and writes its header.
     * The hot path is kept small so that it inlines into the publisher's loop; padding, waiting for space and
     * argument errors are out of line.
     *
     * @return position of the payload
     */
    public long claimSingleMessage(final int claimingPayloadSize,
                                   final long timestamp,
                                   final long correlationId,
                                   final byte messageType) {

        final int fullMessageSize = claimingPayloadSize + MSG_HEADER_SIZE;
        if (messageType < 1 || messageType > MSG_TYPE_POISON_PILL || (correlationId >>> 56) != 0
                || claimingPayloadSize < 0 || fullMessageSize > bufferSize) {
            throw invalidClaim(claimingPayloadSize, correlationId, messageType);
        }

        long msgStart = reservedPosition;
        int index = (int) (msgStart & indexMask);
        if (bufferSize - index < fullMessageSize) {
            msgStart = padToNextLap(msgStart, index, fullMessageSize);
            index = 0;
        } else {
            final long wrapPoint = msgStart + fullMessageSize - bufferSize;
            if (wrapPoint > cachedOutboundPosition) {
                wrapPointCheckWaitUpdate(msgStart, wrapPoint);
            }
        }
        reservedPosition = msgStart + fullMessageSize;

        buffer[index] = (((long) messageType) << 56) | correlationId;
        buffer[index + 1] = timestamp;
        buffer[index + 2] = claimingPayloadSize;
        // buffer[index + 3] is the commit word in HEADER_COMMIT mode, unused with CURSOR
        if (publicationMode == PublicationMode.HEADER_COMMIT) {
            addPending(msgStart);
        }
        return msgStart + MSG_HEADER_SIZE;
    }

    // the message does not fit before the end of the ring: pad the rest of the lap, continue from the next lap
    private long padToNextLap(final long msgStart, final int index, final int fullMessageSize) {
        final long remaining = bufferSize - index;
        wrapPointCheckWaitUpdate(msgStart, msgStart + remaining + fullMessageSize - bufferSize);
        buffer[index] = 0L;
        if (publicationMode == PublicationMode.HEADER_COMMIT && remaining >= MSG_HEADER_SIZE) {
            // padding has no payload: commit it now, readers cannot reach it before the messages ahead of it
            HeaderCommitFence.commit(buffer, index, msgStart);
        }
        return msgStart + remaining;
    }

    private void addPending(final long msgStart) {
        if (pendingTail - pendingHead == PENDING_MAX) {
            commitUpTo(msgStart);
        }
        pendingStarts[(int) (pendingTail++ & (PENDING_MAX - 1))] = msgStart;
    }

    private static IllegalArgumentException invalidClaim(int payloadSize, long correlationId, byte messageType) {
        return new IllegalArgumentException("invalid claim: messageType=" + messageType + " (1.." + MSG_TYPE_POISON_PILL
                + "), correlationId=" + correlationId + " (0..2^56-1), payloadSize=" + payloadSize);
    }

    private void wrapPointCheckWaitUpdate(long msgStartSequence, long wrapPoint) {

        /*                   publishedPosition            nextSequence
        ..............................|........................|.........................................................
                 |
              wrapPoint

        wrapPoint > cachedTailPosition  --- check if filling nextSequence will corrupt unreleased entries
        cachedTailPosition > publishedPosition --- "Handle the extraordinary case of a gating sequence being larger than the cursor more gracefully"
         */

        if (wrapPoint > cachedOutboundPosition) {

            // let processors progress (todo can try do once only if discovered tailFence sill not behind wrap point?)
//            log.debug("setVolatile msgStartSequence={}",msgStartSequence);
            // make everything written so far visible, readers must be able to free space for us
            if (publicationMode == PublicationMode.HEADER_COMMIT) {
                commitUpTo(msgStartSequence);
            } else {
                inboundFence.setVolatile(msgStartSequence);  // StoreLoad fence
            }

//            log.debug(" tailFence.getVolatile()={}",tailFence.getVolatile());
            long minSequence;
            while (wrapPoint > (minSequence = Math.min(releasingFence.getAcquire(cachedOutboundPosition), msgStartSequence))) {
                // parkNanos(1) sleeps ~50us (timer slack) and collapsed saturated throughput; spin instead
                Thread.onSpinWait();
//                Thread.yield();

                tailStrike++;
            }

            cachedOutboundPosition = minSequence;
        }
    }

    public void writeLongData(long sequence, int offset, long value) {
        final int idx = (int) sequence & indexMask;
        buffer[idx + offset] = value;
    }


    public void writeLongData(long sequence, int offset, long value0, long value1, long value2, long value3, long value4, long value5) {
        final int pos = ((int) sequence & indexMask) + offset;
        buffer[pos] = value0;
        buffer[pos + 1] = value1;
        buffer[pos + 2] = value2;
        buffer[pos + 3] = value3;
        buffer[pos + 4] = value4;
        buffer[pos + 5] = value5;
    }


    public void writeLongDataUnsafe(int index, long value) {
        buffer[index] = value;
    }


    /**
     * Makes all messages claimed and written before positionPlusSize visible to readers. Several messages can be
     * published by one call (batching): pass the end of the last one.
     */
    public void publish(long positionPlusSize) {
        if (publicationMode == PublicationMode.HEADER_COMMIT) {
            commitUpTo(positionPlusSize);
        } else {
            inboundFence.setRelease(positionPlusSize);
        }
    }

    // Commits the pending messages that start before end, last one first. A reader that sees the commit word of the first
    // message also sees everything written before it - the other commit words of the batch - and goes through the
    // batch without waiting; in forward order it would chase the publisher message by message, reading each line while
    // the publisher still has to write to it.
    private void commitUpTo(final long end) {
        final long[] starts = pendingStarts;
        long upTo = pendingHead;
        while (upTo != pendingTail && starts[(int) (upTo & (PENDING_MAX - 1))] < end) {
            upTo++;
        }
        for (long i = upTo - 1; i >= pendingHead; i--) {
            final long start = starts[(int) (i & (PENDING_MAX - 1))];
            HeaderCommitFence.commit(buffer, (int) (start & indexMask), start);
        }
        pendingHead = upTo;
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

    @Override
    public void close() throws Exception {


    }
}
