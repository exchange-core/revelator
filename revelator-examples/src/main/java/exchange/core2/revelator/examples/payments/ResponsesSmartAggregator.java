package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsLongBuffer;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ResponsesSmartAggregator implements SimpleMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(ResponsesSmartAggregator.class);

    private final LocalResultsLongBuffer[] resultsBuffers;
    private final IFence[] fencesSt1;
    private final IPaymentsResponseHandler responseHandler;
    private final long[] requestsBuffer;

    private final long handlersMask;

    private int lastAddr;

    private int spinsCounter1 = 0;

    private int spinsCounter2 = 0;

    private int hit1Counter = 0;
    private int hit2Counter = 0;

    private final long[] fencesCache;

    public ResponsesSmartAggregator(final LocalResultsLongBuffer[] resultsBuffers,
                                    final IFence[] fencesSt1,
                                    final long handlersMask,
                                    final IPaymentsResponseHandler responseHandler,
                                    final long[] requestsBuffer) {

        this.resultsBuffers = resultsBuffers;
        this.fencesSt1 = fencesSt1;
        this.responseHandler = responseHandler;
        this.handlersMask = handlersMask;
        this.requestsBuffer = requestsBuffer;
        this.fencesCache = new long[fencesSt1.length];
    }

    @Override
    public void handleMessage(final long[] bufferIgnore,
                              final int index,
                              final int msgSize,
                              final long timestamp,
                              final long globalOffset,
                              final long correlationId,
                              final byte msgType) {

        // TODO incorrect wait logic for MSG_TYPE_TEST_CONTROL
        final long resultsCode = waitAndMergeResult(index, globalOffset, msgType);

        this.lastAddr = index;

        final IPaymentsResponseHandler.IRequestAccessor accessor;
        switch (msgType) {
            case PaymentsApi.CMD_TRANSFER -> accessor = transferAccessor;
            case PaymentsApi.CMD_ADJUST_BALANCE -> accessor = adjustBalanceAccessor;
            case PaymentsApi.CMD_OPEN_ACCOUNT -> accessor = openAccountAccessor;
            case PaymentsApi.CMD_CLOSE_ACCOUNT -> accessor = closeAccountAccessor;
            case PaymentsApi.CMD_CTRL_CUR_RATE, PaymentsApi.CMD_CTRL_FEES -> accessor = unsupportedAccessor;
            case Revelator.MSG_TYPE_TEST_CONTROL -> accessor = testControlCmdAccessor;
            default -> throw new IllegalArgumentException("Unexpected message type " + msgType);
        }

        if (msgType == Revelator.MSG_TYPE_TEST_CONTROL) {
            final long data = requestsBuffer[index];
            if (data == 1073923874826736264L) {
                // log.debug("AGGREGATOR STAT: c1={} c2={} hit1={} hit2={}", spinsCounter1, spinsCounter2, hit1Counter, hit2Counter);
                spinsCounter1 = 0;
                spinsCounter2 = 0;
                hit1Counter = 0;
                hit2Counter = 0;
            }
        }

        responseHandler.commandResult(
                timestamp,
                correlationId,
                (int) resultsCode,
                accessor);
    }

    private long waitAndMergeResult(final int index,
                                    final long globalOffset,
                                    final byte msgType) {

        final long account1 = requestsBuffer[index]; // account - always first field
        final int handlerIdx1 = (int) (account1 & handlersMask);

        // TODO introduce CachingFencesArray class
        if (fencesCache[handlerIdx1] < globalOffset) {
            // wait for first fence
            final IFence fence1 = fencesSt1[handlerIdx1];
            long availableOffset;
            while ((availableOffset = fence1.getAcquire(0)) < globalOffset) {
                Thread.onSpinWait();
//            Thread.yield();
                spinsCounter1++;
            }
            fencesCache[handlerIdx1] = availableOffset;
        } else {
            hit1Counter++;
        }

        // result for first fence has received
        final long result1 = resultsBuffers[handlerIdx1].get(index);
        if (msgType != PaymentsApi.CMD_TRANSFER) {
            // can return result, unless it is transfer command - would need to wait second party
            return result1;
        }

        if (result1 < 0) {
            // can return error (skip waiting other part)
            return result1;
        }

        final long dstAccount = requestsBuffer[index + 1];
        final int handlerIdx2 = (int) (dstAccount & handlersMask);
        if (handlerIdx2 == handlerIdx1) {
            // can return anyway because processing was done by same thread
            return result1;
        }

        if (fencesCache[handlerIdx2] < globalOffset) {
            // wait for second account fence
            final IFence fence2 = fencesSt1[handlerIdx2];
            long availableOffset;
            while ((availableOffset = fence2.getAcquire(0)) < globalOffset) {
                Thread.onSpinWait();
//            Thread.yield();
                spinsCounter2++;
            }
            fencesCache[handlerIdx2] = availableOffset;
        } else {
            hit2Counter++;
        }

        return resultsBuffers[handlerIdx2].get(index);
    }

    private final IPaymentsResponseHandler.IAdjustBalanceAccessor adjustBalanceAccessor = new IPaymentsResponseHandler.IAdjustBalanceAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_ADJUST_BALANCE;
        }

        @Override
        public long getAccount() {
            return requestsBuffer[lastAddr];
        }

        @Override
        public long getAmount() {
            return requestsBuffer[lastAddr + 1];
        }
    };


    private final IPaymentsResponseHandler.ITransferAccessor transferAccessor = new IPaymentsResponseHandler.ITransferAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_TRANSFER;
        }

        @Override
        public long getAccountFrom() {
            return requestsBuffer[lastAddr];
        }

        @Override
        public long getAccountTo() {
            return requestsBuffer[lastAddr + 1];
        }

        @Override
        public long getAmount() {
            return requestsBuffer[lastAddr + 2];
        }

        @Override
        public int getCurrency() {
            throw new UnsupportedOperationException();
        }
    };

    private final IPaymentsResponseHandler.IOpenAccountAccessor openAccountAccessor = new IPaymentsResponseHandler.IOpenAccountAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_OPEN_ACCOUNT;
        }

        @Override
        public long getAccount() {
            return requestsBuffer[lastAddr];
        }
    };

    private final IPaymentsResponseHandler.ICloseAccountAccessor closeAccountAccessor = new IPaymentsResponseHandler.ICloseAccountAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_CLOSE_ACCOUNT;
        }

        @Override
        public long getAccount() {
            return requestsBuffer[lastAddr];
        }
    };

    private final IPaymentsResponseHandler.ITestControlCmdAccessor testControlCmdAccessor = new IPaymentsResponseHandler.ITestControlCmdAccessor() {

        @Override
        public byte getCommandType() {
            return Revelator.MSG_TYPE_TEST_CONTROL;
        }

        @Override
        public int getMsgSize() {
            return (int) requestsBuffer[lastAddr - Revelator.MSG_HEADER_SIZE + 2];
        }

        @Override
        public byte getMsgType() {
            final long header1 = requestsBuffer[lastAddr - Revelator.MSG_HEADER_SIZE];
            final int header2 = (int) (header1 >>> 56);
            return (byte) (header2 & 0x1F);
        }

        @Override
        public long getData(int offset) {
            final int msgSize = getMsgSize();
            if (offset < 0 || offset >= msgSize) {
                throw new IllegalArgumentException("Can not request offset " + offset + " because message size is " + msgSize);
            }
            return requestsBuffer[lastAddr + offset];
        }

        @Override
        public long[] getData() {
            throw new UnsupportedOperationException(); // TODO implement
        }
    };

    private final IPaymentsResponseHandler.IUnsupportedAccessor unsupportedAccessor = () -> {
        throw new UnsupportedOperationException();
    };


    @Override
    public String toString() {
        return "ResponsesSmartAggregator";
    }
}
