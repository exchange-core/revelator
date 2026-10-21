package exchange.core2.revelator.processors.pipelined;


import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/*
 * Message:
 * long sequence
 * long timestamp
 * int length
 *
 * short command
 * long uid
 * long orderId
 *
 *
 *
 *
 */
public class PipelinedFlowProcessor<S extends PipelinedFlowSession> implements IFlowProcessor {

    private static final long WORK_TRIGGER_MAX_VALUE = Long.MAX_VALUE >> 1;

    private static final Logger log = LoggerFactory.getLogger(PipelinedFlowProcessor.class);

    private final int numHandlers;
    private final PipelinedStageHandler<S>[] handlers;
    private final int PIPELINE_SIZE = 64; // should be be 2^N
    private final S[] sessions;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence = new SingleWriterFence();


    private final int indexMask;
    private final long[] buffer;


    private final int[] workWeights;
//    private final int missWeights[];


    private long dataSpinCounter = 0;
    private final long[] missCounters;


    @SuppressWarnings("unchecked")
    public PipelinedFlowProcessor(final List<PipelinedStageHandler<S>> handlers,
                                  final Supplier<S> sessionsFactory,
                                  final IFence inboundFence,
                                  int indexMask,
                                  long[] buffer) {

        this.handlers = handlers.toArray(x -> new PipelinedStageHandler[handlers.size()]);
        this.numHandlers = handlers.size();
        this.sessions = createSessions(sessionsFactory, PIPELINE_SIZE);
        this.workWeights = handlers.stream().mapToInt(PipelinedStageHandler::getHitWorkWeight).toArray();
        this.inboundFence = inboundFence;
        this.indexMask = indexMask;
        this.buffer = buffer;
        this.missCounters = new long[handlers.size()];
    }

    @SuppressWarnings("unchecked")
    private static <S> S[] createSessions(Supplier<S> sessionsFactory, final int size) {
        return (S[]) IntStream.range(0, size)
                .mapToObj(i -> sessionsFactory.get())
                .toArray(x -> new PipelinedFlowSession[size]);
    }


    @Override
    public void run() {

        final int pipelineMask = PIPELINE_SIZE - 1;


        final long[] handlerWorkTriggers = new long[numHandlers];
//        long headWorkTrigger = 0L;
        long workCounter = 0L;

        long headSequence = -1L; // processed (last known) sequence
        long tailSequence = -1L;

        // processed (last known) sequence by each handler, starting with -1 (nothing processed)
        final long[] handlerSequence = new long[numHandlers];
        for (int i = 0; i < numHandlers; i++) handlerSequence[i] = -1;

        long initializerOffset = 0L; // session initializer global offset
        long nextAvailableOffset;

        boolean isShutdown = false;

        long lastOffsetToRelease = -1L;

        while (true) {

            // always work counter
            workCounter++;

            // gatingSequence is a barrier to protect sessions queue from wrapping:
            final long gatingSequence = headSequence + PIPELINE_SIZE;

//            log.info("------- new cycle, tailSequence={} headSequence={} gatingSequence={} ", tailSequence, headSequence, gatingSequence);

            if ((tailSequence == headSequence)) {
                if (isShutdown) {
                    log.info("All sessions processed, processor stopped");
                    return;
                } else {
                    releasingFence.setRelease(lastOffsetToRelease);
                }
            }

            // check for new messages or init new sessions only if there free space in the cyclic sessions buffer
            if (tailSequence < gatingSequence) {
//            if (tailSequence == headSequence) { // TODO affects latency!!

                // try to wait for incomingFence if it is time for that
                // availableOffset will become larger than initializerOffset if new messages arrived
//                if (workCounter >= headWorkTrigger) {

                // check fence once if batch is not completed yet (no need to spin here)
                // otherwise spin on fence volatile read
                //do {
                //  workCounter++;
                nextAvailableOffset = inboundFence.getAcquire(Long.MIN_VALUE);

                if (tailSequence == headSequence && initializerOffset == nextAvailableOffset) {
                    while (initializerOffset == (nextAvailableOffset = inboundFence.getAcquire(Long.MIN_VALUE))) {
//                        Thread.yield();
                        Thread.onSpinWait();
//                        LockSupport.parkNanos(1L);
                        dataSpinCounter++;
                    }
                }

//                if(initializerOffset == nextAvailableOffset){
//                    Thread.yield();
//                }

                //Thread.onSpinWait();
                //} while (availableOffset <= initializerOffset);

                // set to known head offset
//                }

//                log.debug("initializerOffset={} nextAvailableOffset={}", initializerOffset, nextAvailableOffset);

                // parse new messages if there are some
                while (initializerOffset < nextAvailableOffset) {

//                    log.debug("NEW SESSION: initializerOffset={} nextAvailableOffset={}", initializerOffset, nextAvailableOffset);

//            log.debug("reading at index={} ({}<{})",(int) (positionSeq & indexMask), positionSeq , availableSeq);

                    final int index = (int) (initializerOffset & indexMask);

                    final long header1 = buffer[index];

//                    log.debug("header1 = {}", header1);

                    if (header1 == 0L) {
                        // empty message - skip until end of the buffer
                        initializerOffset = (initializerOffset | indexMask) + 1;
//                        log.debug("empty message - skip until end of the buffer");
                        continue;
                    }

                    tailSequence++;

//                    log.debug("tailSequence={}", tailSequence);

                    final int sessionIdx = (int) tailSequence & pipelineMask;

//                    log.debug("sessionIdx={}", sessionIdx);
                    final S session = sessions[sessionIdx];

                    session.bufferIndex = index + Revelator.MSG_HEADER_SIZE;
                    session.correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;

                    final int header2 = (int) (header1 >>> 56);

//                    log.debug("header2={}", header2);
                    session.messageType = (byte) (header2 & 0x1F);

//                    log.debug("session.messageType={}", session.messageType);

                    // TODO throw shutdown signal exception

                    session.timestamp = buffer[index + 1];

//                    log.debug("session.timestamp={}", session.timestamp);

                    final long payloadSize = buffer[index + 2];
                    session.payloadSize = (int) payloadSize;

                    session.globalOffset = initializerOffset + payloadSize + Revelator.MSG_HEADER_SIZE;
//                    log.debug("initializerOffset={} -> globalOffset={}", initializerOffset, session.globalOffset);


                    initializerOffset += Revelator.MSG_HEADER_SIZE + session.payloadSize;

                    if (tailSequence == gatingSequence) {
//                        log.info("gatingSequence reached = {}", gatingSequence);
//                        session.wordsLeftInBatch = 0;
                        break;
                    }

                    // NOTE: (only if wordsLeftInBatch feature is enabled)
                    // it is very important to write meaningful message after empty message because
                    // batch-aggregation logic in handlers relies on assumption that last message is always non-empty

//                    session.wordsLeftInBatch = (int) (nextAvailableOffset - initializerOffset);

                    if (session.messageType == Revelator.MSG_TYPE_POISON_PILL) {

                        log.info("Shutdown signal received, processing all remaining sessions...");
                        isShutdown = true;
                        break;
                    }

                    if (session.messageType == Revelator.MSG_TYPE_TEST_CONTROL) {
                        final long data = buffer[index + 3];
                        if (data == 1073923874826736264L) {
                            //log.debug("{} spin:{} miss:{}", this, dataSpinCounter, Arrays.toString(missCounters));
                            Arrays.fill(missCounters, 0L);
                            dataSpinCounter = 0L;
                        }
                    }

                }
            } else {
//                log.debug("Skip:  space in the cyclic sessions buffer");
            }

//            if(headSequence == tailSequence){
//                // nothing to process
//                continue;
//            }

//            log.info("---------- PROCESSING BETWEEN tailSequence={} and headSequence={} --------", tailSequence, headSequence);

            // handlers cycle - each handler processed once
            // handler skip if was unsuccessfully processed recently (don't spin, do another work instead)

            // starting from head of sessions queue
            long prevHandlerSequence = tailSequence;

            for (int handlerIdx = 0; handlerIdx < numHandlers; handlerIdx++) {

                long sequence = handlerSequence[handlerIdx];

//                log.debug("handler{}seq={} prevHandlerSequence={}", handlerIdx, sequence, prevHandlerSequence);


                // never can go in front of previous handler
                // first handler has 'tailSequence' as a barrier, meaning it can only process messages with session
                if (sequence < prevHandlerSequence) {

//                    log.debug("processing handler {}, workCounter={} handlerWorkTriggers[{}]={}", handlerIdx, workCounter, handlerIdx, handlerWorkTriggers[handlerIdx]);

                    // check if it is time to process
                    if (workCounter >= handlerWorkTriggers[handlerIdx]) {

                        // increment work counter to indicate some work was done
                        final long workWeight = workWeights[handlerIdx];
                        workCounter += workWeight;

                        sequence++;
                        // extract next message session and try to process it
                        final int sessionIdx = (int) (sequence) & pipelineMask;

                        final S session = sessions[sessionIdx];
//                        log.debug("sessionIdx={} go={} t={}", sessionIdx, session.globalOffset, session.timestamp);

                        final boolean success = handlers[handlerIdx].process(session);

//                        log.debug("success={}", success);

                        if (success) {
                            // successful processing - move sequence forward and update it
                            handlerSequence[handlerIdx] = sequence;

                            // every time last handler makes progress - update outgoingFence
                            // TODO check if it slows down compared to publishing once-by-batch in disruptor
                            if (handlerIdx == numHandlers - 1) {
//                                releasingFence.setRelease(session.globalOffset);
                                lastOffsetToRelease = session.globalOffset;
                            }

                        } else {
                            // not - postpone next call by some constant
                            handlerWorkTriggers[handlerIdx] = workCounter + (workWeight << 2);
                            sequence--;

                            missCounters[handlerIdx]++;
                        }

                    } else {
                        // always make some progress
                        workCounter++;
                    }
                }

                prevHandlerSequence = sequence;
            }

            // updating tail (processed by last handler)
            headSequence = prevHandlerSequence;

//            log.debug("headSequence updated to {}", headSequence);


            // work counters wrapping protection
//            if (workCounter > WORK_TRIGGER_MAX_VALUE) {
//                workCounter -= WORK_TRIGGER_MAX_VALUE;
//                for (int handlerIdx = 0; handlerIdx < numHandlers; handlerIdx++) {
//                    if (handlerWorkTriggers[handlerIdx] > 0) {
//                        handlerWorkTriggers[handlerIdx] -= WORK_TRIGGER_MAX_VALUE;
//                    }
//                }
//            }

        }


    }

    @Override
    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }


//    long waitMessage(final long lastSeq) {
//        return 0L;
//    }
//
//    long tryWaitMessage(final long lastSeq) {
//        return 0L;
//    }


    @Override
    public String toString() {
        return "PipelinedFlowProcessor{" +
                Arrays.toString(handlers) +
                '}';
    }
}
