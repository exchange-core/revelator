package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SimplePaymentsHandler implements SimpleMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(SimplePaymentsHandler.class);


    private final AccountsProcessor accountsProcessor;
    private final LocalResultsByteBuffer resultsBuffer;

    public SimplePaymentsHandler(AccountsProcessor accountsProcessor,
                                 LocalResultsByteBuffer resultsBuffer) {
        this.accountsProcessor = accountsProcessor;
        this.resultsBuffer = resultsBuffer;
    }

    @Override
    public void handleMessage(final long[] buffer,
                              final int addr,
                              final int msgSize,
                              final long timestamp,
                              final long globalOffset,
                              final long correlationId,
                              final byte msgType) {

        //log.debug("Handle message correlationId={}", correlationId);

        switch (msgType) {

            case PaymentsApi.CMD_OPEN_ACCOUNT -> {

                final long account = buffer[addr];
                final long secret = buffer[addr + 1];

                if (!accountsProcessor.accountExists(account)) {
//            log.debug("Opening account {}", account);
                    accountsProcessor.openNewAccount(account, secret);
                    resultsBuffer.set(addr, (byte) 1);
                } else {

                    log.warn("Account {} already exists!", account);
                    resultsBuffer.set(addr, (byte) -1);
                }
            }

            case PaymentsApi.CMD_TRANSFER -> {
                final long accountFrom = buffer[addr];
                final long accountTo = buffer[addr + 1];
                final long amount = buffer[addr + 2];

                final boolean success = accountsProcessor.transferLocally(accountFrom, accountTo, amount, amount);
                resultsBuffer.set(addr, success ? (byte) 1 : -1);
            }

            case PaymentsApi.CMD_ADJUST_BALANCE -> {
                final long account = buffer[addr];
                final long amount = buffer[addr + 1];

                if (!accountsProcessor.accountExists(account)) {

                    log.warn("Account {} does not exists or closed!", account);

                    // account does not exist or closed
                    resultsBuffer.set(addr, (byte) -2);

                } else {
                    boolean success = (amount > 0)
                            ? accountsProcessor.deposit(account, amount)
                            : accountsProcessor.withdrawal(account, -amount);

                    resultsBuffer.set(addr, success ? (byte) 1 : -1);
                }
            }
        }


    }

}
