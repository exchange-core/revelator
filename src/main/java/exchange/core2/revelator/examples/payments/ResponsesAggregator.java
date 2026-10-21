package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;

public final class ResponsesAggregator implements SimpleMessageHandler {

    private final LocalResultsByteBuffer resultsBuffer;
    private final IPaymentsResponseHandler responseHandler;


    private int lastAddr;

    private long[] requestsBuffer;

    public ResponsesAggregator(LocalResultsByteBuffer resultsBuffer,
                               IPaymentsResponseHandler responseHandler) {

        this.resultsBuffer = resultsBuffer;
        this.responseHandler = responseHandler;
    }

    @Override
    public void handleMessage(final long[] buffer,
                              final int index,
                              final int msgSize,
                              final long timestamp,
                              final long globalOffset,
                              final long correlationId,
                              final byte msgType) {

        final int resultsCode = resultsBuffer.get(index);

        this.lastAddr = index;
        this.requestsBuffer = buffer;

        final IPaymentsResponseHandler.IRequestAccessor accessor;
        switch (msgType) {
            case PaymentsApi.CMD_TRANSFER -> accessor = transferAccessor;
            case PaymentsApi.CMD_ADJUST_BALANCE -> accessor = adjustBalanceAccessor;
            case PaymentsApi.CMD_OPEN_ACCOUNT -> accessor = openAccountAccessor;
            case PaymentsApi.CMD_CLOSE_ACCOUNT -> accessor = closeAccountAccessor;
            case Revelator.MSG_TYPE_TEST_CONTROL -> accessor = testControlCmdAccessor;
            default -> throw new IllegalArgumentException("Unexpected message type " + msgType);
        }

        responseHandler.commandResult(
                timestamp,
                correlationId,
                resultsCode,
                accessor);
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


}
