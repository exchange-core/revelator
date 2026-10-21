package exchange.core2.revelator.examples.payments;

import org.eclipse.collections.impl.map.mutable.primitive.IntDoubleHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CurrencyRateProcessor {

    private static final Logger log = LoggerFactory.getLogger(CurrencyRateProcessor.class);


    private final IntDoubleHashMap currencyRates = new IntDoubleHashMap();

    public double getRate(short currencyFrom, short currencyTo) {
        if (currencyFrom == currencyTo) {
            return 1.0;
        } else {
            return currencyRates.get(makeKey(currencyFrom, currencyTo));
        }
    }

    public long convertRate(long amountFrom, short currencyFrom, short currencyTo) {

        if (currencyFrom == currencyTo) {
            return amountFrom;
        }

        final double rate = currencyRates.get(
                makeKey(currencyFrom, currencyTo));

        if (rate == 0.0) {
            log.debug("no currency rate {} -> {}", currencyFrom, currencyTo);
            return -1;
        }


//        final int multiplier = (int) (encodedRate & 0x7FFF_FFFF);
//        final byte orderBefore = (byte) (encodedRate >> 32);
//        final byte orderAfter = (byte) (encodedRate >> 48);
//
//        final long amountTo = ArithmeticUtils.reciprocalMul(amountFrom, orderBefore, multiplier, orderAfter);

        final long amountTo = ArithmeticUtils.multiplyByRate(amountFrom, rate);

        return amountTo;
    }


    public void updateRate(short currencyFrom, short currencyTo, double rate) {

//        log.debug("Updated rate: {}->{} : {}", currencyFrom, currencyTo, rate);

        currencyRates.put(makeKey(currencyFrom, currencyTo), rate);
    }

    private int makeKey(short currencyFrom, short currencyTo) {
        return (currencyFrom << 16) + currencyTo;
    }

    public void exportAllRates(RatesConsumer consumer) {
        currencyRates.forEachKeyValue((k, rate) -> consumer.accept((short) (k >> 16), (short) (k & 0x7FFF), rate));
    }

    @FunctionalInterface
    public interface RatesConsumer {
        void accept(short currencyFrom, short currencyTo, double rate);
    }
}
