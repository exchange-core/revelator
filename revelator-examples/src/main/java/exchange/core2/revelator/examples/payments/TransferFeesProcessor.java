package exchange.core2.revelator.examples.payments;

import org.eclipse.collections.impl.map.mutable.primitive.ShortLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ShortObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransferFeesProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransferFeesProcessor.class);

    private final AccountsProcessor accountsProcessor;
    private final CurrencyRateProcessor currencyRateProcessor;

    private double feeK = 0.0;
    private final ShortObjectHashMap<FeeConfig> currencyFees = new ShortObjectHashMap<>();
    private final ShortLongHashMap treasures = new ShortLongHashMap();


    public TransferFeesProcessor(final CurrencyRateProcessor currencyRateProcessor,
                                 final AccountsProcessor accountsProcessor) {

        this.currencyRateProcessor = currencyRateProcessor;
        this.accountsProcessor = accountsProcessor;
    }


    public boolean performWithdrawal(final TransferSession session,
                                     final TransferType transferType,
                                     final long accountSrc,
                                     final long accountDst,
                                     final long orderAmount,
                                     final short orderCurrency) {

        return switch (transferType) {

            case DESTINATION_EXACT -> processDstExactInit(
                    orderAmount,
                    orderCurrency,
                    accountSrc,
                    accountDst,
                    session);

            case SOURCE_EXACT -> processSrcExactInit(
                    orderAmount,
                    orderCurrency,
                    accountSrc,
                    accountDst,
                    session);
        };
    }

    /**
     * /-<-convert--<--<-<-----------------\
     * 11623 JPY (FEE) -> 11523 JPY  ........  100.00 USD ---> 85.73 EUR
     * Calculate source amount and withdraw it
     * Otherwise return false
     */
    public boolean processDstExactInit(final long orderAmount,
                                       final short orderCurrency,
                                       final long accountSrc,
                                       final long accountDst,
                                       final TransferSession session) {

        final short currencyDst = AccountsProcessor.extractCurrency(accountDst);

        // calculate destination amount

        final long amountDst = (currencyDst == orderCurrency)
                ? orderAmount
                : currencyRateProcessor.convertRate(orderAmount, orderCurrency, currencyDst);

        if (amountDst == -1L) {
            log.warn("Can not convert currency");
            return false;
        }

        if (amountDst == 0) {
            log.warn("can not transfer just 0");

            // can not transfer just 0
            return false;
        }

        // calculate source amount based on order amount

        final short currencySrc = AccountsProcessor.extractCurrency(accountSrc);

        final long amountSrc = (currencyDst == currencySrc)
                ? amountDst
                : currencyRateProcessor.convertRate(amountDst, currencyDst, currencySrc);

        if (amountSrc == -1L) {
            log.warn("Can not convert currency");
            return false;
        }

        // apply fee to the calculated source amount
        final long srcFee = calculateFee(amountSrc, currencySrc);

        final long amountSrcWithFee = amountSrc + srcFee;

        final boolean withdrawalSucceeded = accountsProcessor.withdrawal(accountSrc, amountSrcWithFee);

        // Check for NSF
        if (!withdrawalSucceeded) {
            log.warn("NSF");
            return false;
        }

        if (currencyDst == currencySrc) {
            session.treasureAmountSrc = srcFee;
            session.treasureAmountDst = 0L;
        } else {
            // update treasures for SOURCE -> DST conversion
            session.treasureAmountSrc = amountSrcWithFee;
            session.treasureAmountDst = -amountDst;
        }

        session.amountSrc = amountSrc;
        session.amountDst = amountDst;

        return true;
    }


    /**
     * /-<-convert--<--<-----<-------<-----\
     * 11523 JPY  ........  100.00 USD ---> 85.73 EUR ----> 85.23 EUR (FEE)
     * Calculate source amount and withdraw it
     * Otherwise return false
     */
    public boolean processSrcExactInit(final long orderAmount,
                                       final short orderCurrency,
                                       final long accountSrc,
                                       final long accountDst,
                                       final TransferSession session) {

        final short currencyDst = AccountsProcessor.extractCurrency(accountDst);

        // calculate destination amount

        final long amountDst = (currencyDst == orderCurrency)
                ? orderAmount
                : currencyRateProcessor.convertRate(orderAmount, orderCurrency, currencyDst);

        if (amountDst == -1L) {
            log.warn("Can not convert currency");
            return false;
        }


        // apply fee to the calculated destination amount
        final long dstFee = calculateFee(amountDst, currencyDst);
        final long amountDstAfterFee = amountDst - dstFee;

        if (amountDstAfterFee <= 0) {
            // can not transfer just 0 or negative
            log.warn("Amount after fee is 0 or less: amountDst={} dstFee={}", amountDst, dstFee);
            log.warn("ORD:{}:{} -> {}-DST:{}:{} - {} = {}", orderAmount, orderCurrency, accountDst, amountDst, currencyDst,  dstFee, amountDstAfterFee);
            return false;
        }

        // calculate source amount based on order amount

        final short currencySrc = AccountsProcessor.extractCurrency(accountSrc);

        final long amountSrc = (currencyDst == currencySrc)
                ? amountDst
                : currencyRateProcessor.convertRate(amountDst, currencyDst, currencySrc);

        if (amountSrc == -1L) {
            log.warn("Can not convert currency");
            return false;
        }


        final boolean withdrawalSucceeded = accountsProcessor.withdrawal(accountSrc, amountSrc);

        // Check for NSF
        if (!withdrawalSucceeded) {
            log.warn("NSF");
            return false;
        }

        if (currencyDst == currencySrc) {
            session.treasureAmountSrc = 0L;
            session.treasureAmountDst = dstFee; // fee in destination currency
        } else {
            // update treasures for SOURCE -> DST conversion
            session.treasureAmountSrc = amountSrc; // source amount
            session.treasureAmountDst = -amountDstAfterFee; // destination amount, holding fee
        }

        session.amountSrc = amountSrc;
        session.amountDst = amountDst;

        return true;
    }

    public void applyTreasures(short currencySrc,
                               short currencyDst,
                               TransferSession session) {

        if (session.treasureAmountSrc != 0) {
            treasures.addToValue(currencySrc, session.treasureAmountSrc);
        }
        if (session.treasureAmountDst != 0) {
            treasures.addToValue(currencyDst, session.treasureAmountDst);
        }
    }

    private long calculateFee(final long amount, final short currency) {

        final FeeConfig feeConfig = currencyFees.get(currency);

        if (feeConfig == null) {
            log.debug("no fee configuration for currency {}", currency);

            throw new IllegalStateException("no fee configuration for currency " + currency);
            //return -1;
        }

//        final long feeRaw = ArithmeticUtils.reciprocalMul(amount, feeOrderBefore, feeMultiplier, feeOrderAfter);


        final long feeRaw = ArithmeticUtils.multiplyByRate(amount, feeK);

        final long feeLimited = Math.max(feeConfig.minFee, Math.min(feeConfig.maxFee, feeRaw));

        return feeLimited;
    }


    public void setFeeK(final double feeK) {

        this.feeK = feeK;
    }

    public void putFeeConfig(final short currency,
                             final long minFee,
                             final long maxFee) {

        currencyFees.put(currency, new FeeConfig(minFee, maxFee));
    }

    public void updateCurrencyRate(short currencyFrom, short currencyTo, double rate) {
        currencyRateProcessor.updateRate(currencyFrom, currencyTo, rate);
    }


    public record FeeConfig(long minFee, long maxFee) {
    }


}
