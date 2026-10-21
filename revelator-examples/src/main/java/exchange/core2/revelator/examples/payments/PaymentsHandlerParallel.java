package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.buffers.LocalResultsLongBuffer;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO implement second part (disruptor design)
public final class PaymentsHandlerParallel implements SimpleMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentsHandlerParallel.class);

    private final AccountsProcessor accountsProcessor;
    private final LocalResultsLongBuffer resultsBuffer;

    private final int handlerIndex;
    private final long handlersMask;

    private long useless = 0;

    public PaymentsHandlerParallel(AccountsProcessor accountsProcessor,
                                   LocalResultsLongBuffer resultsBuffer,
                                   int handlerIndex,
                                   long handlersMask) {

        this.accountsProcessor = accountsProcessor;
        this.resultsBuffer = resultsBuffer;
        this.handlerIndex = handlerIndex;
        this.handlersMask = handlersMask;
    }

    @Override
    public void handleMessage(long[] buffer,
                              int addr,
                              int msgSize,
                              long timestamp,
                              long globalOffset,
                              long correlationId,
                              byte msgType) {

        switch (msgType) {

            case PaymentsApi.CMD_TRANSFER -> {
                final long accountFrom = buffer[addr];
                final long accountTo = buffer[addr + 1];
                final long amount = buffer[addr + 2];
                final TransferType transferType = TransferType.fromByte((byte) buffer[addr + 3]);
                processTransfer1(accountFrom, accountTo, amount, transferType, handlerIndex);
            }

            case PaymentsApi.CMD_OPEN_ACCOUNT -> {
                final long account = buffer[addr];
                final long secret = buffer[addr + 1];
                processOpenAccount(account, secret, handlerIndex);
            }

            case PaymentsApi.CMD_ADJUST_BALANCE -> {
                final long account = buffer[addr];
                final long amount = buffer[addr + 1];
                processAdjustment(account, amount, handlerIndex);
            }

            case Revelator.MSG_TYPE_TEST_CONTROL, Revelator.MSG_TYPE_POISON_PILL -> {
                //
            }

            default -> throw new IllegalStateException("Unsupported message type " + msgType + " at offset " + globalOffset);
        }
    }


    private void processOpenAccount(final long account,
                                    final long secret,
                                    final int index) {

        if ((account & handlersMask) != handlerIndex) {
            return;
        }

        if (!accountsProcessor.accountExists(account)) {
//            log.debug("Opening account {}", account);
            accountsProcessor.openNewAccount(account, secret);
            resultsBuffer.set(index, (byte) 1);
        } else {

            log.warn("Account {} already exists!", account);
            resultsBuffer.set(index, (byte) -1);
        }
    }


    private void processAdjustment(final long account,
                                   final long amount,
                                   final int index) {

        if ((account & handlersMask) != handlerIndex) {
            return;
        }

        if (!accountsProcessor.accountExists(account)) {

            log.warn("Account {} does not exists or closed!", account);
            log.warn("Useless {} ", useless);

            // account does not exist or closed
            resultsBuffer.set(index, (byte) -2);
            return;
        }

        boolean success = (amount > 0)
                ? accountsProcessor.deposit(account, amount)
                : accountsProcessor.withdrawal(account, -amount);

        resultsBuffer.set(index, success ? (byte) 1 : -1);
    }

    private void processTransfer1(final long accountSrc,
                                  final long accountDst,
                                  final long amount,
                                  final TransferType transferType,
                                  final int index) {

        final boolean processSrc = (accountSrc & handlersMask) == handlerIndex;
        final boolean processDst = (accountDst & handlersMask) == handlerIndex;

        if (!processSrc && !processDst) {
            // message is not related to this handler - just skip it
            return;
        }

//        int h = Hashing.hash(accountDst);
//        for (int i = 0; i < 60; i++) {
//            h = Hashing.hash(h);
//        }
//        useless += h;

        final boolean success;

        if (processSrc && processDst) {
            // source and destination both handled by this processor
            success = accountsProcessor.transferLocally(accountSrc, accountDst, amount, amount);

        } else if (processSrc) {

            success = accountsProcessor.withdrawal(accountSrc, amount);

        } else {

            success = accountsProcessor.deposit(accountDst, amount);
        }

        if (!success) {
            log.warn("Can not process transfer {}->{}! (process {}->{})", accountSrc, accountDst, processSrc, processDst);
        }

        resultsBuffer.set(index, success ? (byte) 1 : -1);
    }

    @Override
    public String toString() {
        return "PaymentsHandler(" + handlerIndex + ')';
    }

}
