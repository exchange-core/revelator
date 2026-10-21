package exchange.core2.revelator;

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
import java.util.concurrent.locks.LockSupport;

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

    private final List<Thread> threads = new ArrayList<>();

    private long reservedPosition = 0L; // nextValue = single writer sequencer position in Disruptor

    private long cachedOutboundPosition = 0L; // cachedValue = min gating sequence in Disruptor

    private long tailStrike = 0L;

    private CompletableFuture<Void> shutdownFuture;

    public static Revelator create(final int bufferSize,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory) {

        if (!BitUtil.isPowerOfTwo(bufferSize)) {
            throw new IllegalArgumentException("Revelator buffer size must be 2^N");
        }

        if (bufferSize < 1024) {
            throw new IllegalArgumentException("Revelator buffer size must be > 1024 bytes");
        }


        final SingleWriterFence inboundFence = new SingleWriterFence(); // single publisher

        final int indexMask = bufferSize - 1;

        final long[] buffer = new long[bufferSize];

        final IFlowProcessorsFactory.ProcessorsChain chain = flowProcessorsFactory.createProcessors(
                inboundFence,
                new RevelatorConfig(indexMask, bufferSize, buffer));


        return new Revelator(
                bufferSize,
                indexMask,
                buffer,
                chain.getProcessors(),
                threadFactory,
                inboundFence,
                chain.getReleasingFence());
    }


    private Revelator(final int bufferSize,
                      final int indexMask,
                      final long[] buffer,
                      final List<? extends IFlowProcessor> processors,
                      final ThreadFactory threadFactory,
                      final SingleWriterFence inboundFence,
                      final IFence outboundFence) {

        this.bufferSize = bufferSize;
        this.indexMask = indexMask;
        this.buffer = buffer;
        this.processors = processors;
        this.threadFactory = threadFactory;
        this.inboundFence = inboundFence;
        this.releasingFence = outboundFence;
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
     * Claim space for single message
     *
     * @param claimingPayloadSize
     * @return offset to write message body
     */
    public long claimSingleMessage(final int claimingPayloadSize,
                                   final long timestamp,
                                   final long correlationId,
                                   final byte messageType) {

        if (messageType < 1 || messageType > MSG_TYPE_POISON_PILL) {
            throw new IllegalArgumentException("message type should be in range: 1.." + MSG_TYPE_POISON_PILL);
        }

        if ((correlationId >> 56) != 0) {
            throw new IllegalArgumentException("correlationId should be in range: 0..2^56-1");
        }

        // calculate expected message size
        final int fullMessageSize = claimingPayloadSize + MSG_HEADER_SIZE;

        if (claimingPayloadSize < 0 || fullMessageSize > bufferSize) {
            throw new IllegalArgumentException("claimed size must be >= 0 and < bufferSize");
        }

        long msgStartSequence = reservedPosition;
        this.reservedPosition += fullMessageSize;
        final long wrapPoint = this.reservedPosition - bufferSize;

        // check if new message can fit into remaining buffer
        int index = (int) (msgStartSequence & indexMask);
        final long remainingSpaceBytes = bufferSize - index;
        if (remainingSpaceBytes < fullMessageSize) {
            // can not fit - write empty message that will be ignored by headers

//            log.debug("Can not fit message because msgStartSequence&mask={} fullMessageSize={} bufferSize={} : SKIP remainingSpaceBytes={}",
//                    index, fullMessageSize, bufferSize, remainingSpaceBytes);

            // write 0 message, indicating that reader should start from buffer
            // there is always at least 8 bytes available due to alignment (only need to check wrap point before writing)
            // so we check wrap point for claimed message (just to do it once)
            wrapPointCheckWaitUpdate(msgStartSequence, wrapPoint + remainingSpaceBytes);
            buffer[index] = 0L;

            index = 0;
            msgStartSequence += remainingSpaceBytes;
            this.reservedPosition += remainingSpaceBytes;
        } else {

//        log.debug("msgStartSequence={} new reservedPosition={} wrapPoint={}",
//                msgStartSequence, reservedPosition, wrapPoint);

            wrapPointCheckWaitUpdate(msgStartSequence, wrapPoint);
        }

//        log.debug("WRITING HEADER correlationId={}", correlationId);

        // write header

        final long msgTypeEncoded = ((long) messageType) << 56;

        // TODO put UserCookie (4bytes), size (2bytes - 512K max msg size)

        buffer[index] = msgTypeEncoded | correlationId;
        buffer[index + 1] = timestamp;
        buffer[index + 2] = claimingPayloadSize;
        buffer[index + 3] = 0L;

        final long payloadStartSeq = msgStartSequence + MSG_HEADER_SIZE;

//        log.debug("WRITING HEADER DONE payloadStartSeq={} index={} claimingPayloadSize={}",
//                payloadStartSeq, payloadStartSeq & indexMask, claimingPayloadSize);

        return payloadStartSeq;
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
            inboundFence.setVolatile(msgStartSequence);  // StoreLoad fence

//            log.debug(" tailFence.getVolatile()={}",tailFence.getVolatile());
            long minSequence;
            while (wrapPoint > (minSequence = Math.min(releasingFence.getAcquire(cachedOutboundPosition), msgStartSequence))) {
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin? (can cause starvation)
//                Thread.onSpinWait();
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


    public void publish(long positionPlusSize) {
//        log.debug("PUBLISH positionPlusSize={}", positionPlusSize);
        inboundFence.setRelease(positionPlusSize);
        // todo waitStrategy.signalAllWhenBlocking();
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
