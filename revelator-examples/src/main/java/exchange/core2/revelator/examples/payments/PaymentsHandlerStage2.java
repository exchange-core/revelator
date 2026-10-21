package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.buffers.LocalResultsLongBuffer;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.processors.pipelined.PipelinedStageHandler;
import org.agrona.collections.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaymentsHandlerStage2 implements PipelinedStageHandler<TransferSession> {

    private static final Logger log = LoggerFactory.getLogger(PaymentsHandlerStage2.class);

    private final AccountsProcessor accountsProcessor;
    private final TransferFeesProcessor transferFeesProcessor;

    private final LocalResultsLongBuffer[] resultsBuffers;

    private final LongHashSet lockedAccounts;


    private final IFence[] fencesSt1;

    private final int handlerIndex;
    private final long handlersMask;

    public PaymentsHandlerStage2(AccountsProcessor accountsProcessor,
                                 TransferFeesProcessor transferFeesProcessor,
                                 LocalResultsLongBuffer[] resultsBuffers,
                                 LongHashSet lockedAccounts,
                                 IFence[] fencesSt1,
                                 int handlerIndex,
                                 long handlersMask) {

        this.accountsProcessor = accountsProcessor;
        this.transferFeesProcessor = transferFeesProcessor;
        this.resultsBuffers = resultsBuffers;
        this.lockedAccounts = lockedAccounts;
        this.fencesSt1 = fencesSt1;
        this.handlerIndex = handlerIndex;
        this.handlersMask = handlersMask;
    }

    @Override
    public boolean process(final TransferSession session) {

//        log.debug("ST2 t={}", session.timestamp);

        // only transfer command can possibly require post-processing
        if (session.messageType == PaymentsApi.CMD_TRANSFER) {
            return processTransfer(session);
        } else {
            return true;
        }
    }

    private boolean processTransfer(final TransferSession session) {

        // if message is not related to this handler - just skip it
        if (!session.processSrc && !session.processDst) {
            return true;
        }

        final short currencySrc = AccountsProcessor.extractCurrency(session.accountSrc);
        final short currencyDst = AccountsProcessor.extractCurrency(session.accountDst);

        // source and destination both handled by this processor
        if (session.processSrc && session.processDst) {

            // no need to revert, but need unlock accounts
            lockedAccounts.remove(session.accountDst);
            lockedAccounts.remove(session.accountSrc);

            transferFeesProcessor.applyTreasures(currencySrc, currencyDst, session);
            return true;
        }

        // only one account is processed by this handler
        // get status of other account  processing
        final long otherAccount = session.processSrc ? session.accountDst : session.accountSrc;
        final int otherIdx = (int) (otherAccount & handlersMask);

        // check Stage 1 progress for particular handler
        final IFence fence = fencesSt1[otherIdx];
        final long progress = fence.getAcquire(-1L);// ignore
        if (progress < session.globalOffset) { // TODO introduce static method (to make it easy to understand)
            // Stage 1 is not completed yet by other handler - can not progress
            return false;
        }

        // get result code for other half of transaction
        final LocalResultsLongBuffer buf = resultsBuffers[otherIdx];
        final long exchangeData = buf.get(session.bufferIndex);

        if (session.processDst) {
            // process destination only
            if (exchangeData >= 0L && session.localPartSucceeded) {
                // settle Destination
                accountsProcessor.deposit(session.accountDst, exchangeData);
            }

            lockedAccounts.remove(session.accountDst);

        } else {
            // process source only
            if (session.localPartSucceeded) {
                if (exchangeData == 0L) {
                    // settle fees
                    transferFeesProcessor.applyTreasures(currencySrc, currencyDst, session);
                } else {
                    // rollback transaction
                    accountsProcessor.balanceCorrection(session.accountSrc, session.amountSrc);
                }
            } else {
                // do nothing if local part not succeeded, because other party was only checking dst account existence
            }

            lockedAccounts.remove(session.accountSrc);
        }

        return true;
    }

    @Override
    public int getHitWorkWeight() {
        return 5;
    }

    @Override
    public String toString() {
        return "ST2(" + handlerIndex + ')';
    }
}
