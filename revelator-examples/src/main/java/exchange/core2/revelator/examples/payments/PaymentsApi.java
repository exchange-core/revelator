package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.Revelator;
import org.agrona.collections.MutableInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class PaymentsApi {

    public static final byte CMD_TRANSFER = (byte) 3;
    public static final byte CMD_ADJUST_BALANCE = (byte) 5;
    public static final byte CMD_OPEN_ACCOUNT = (byte) 7;
    public static final byte CMD_CLOSE_ACCOUNT = (byte) 9;
    public static final byte CMD_CTRL_CUR_RATE = (byte) 13;
    public static final byte CMD_CTRL_FEES = (byte) 14;
    public static final byte CMD_CTRL_TREASURE = (byte) 15;

    // TODO add reports

    public static final byte QRY_GET_BALANCE = (byte) 20;

    private static final Logger log = LoggerFactory.getLogger(PaymentsApi.class);

    private final Revelator revelator;
    private final int indexMask;


    public PaymentsApi(Revelator revelator, int indexMask) {
        this.revelator = revelator;
        this.indexMask = indexMask;
    }

    public void transfer(final long timestamp,
                         final long correlationId,
                         final long accountSrc,
                         final long accountDst,
                         final long amount,
                         final short currency,
                         final TransferType transferType,
                         final long[] sha256) {

        final int msgSize = 8;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_TRANSFER);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, accountSrc);
        revelator.writeLongDataUnsafe(index + 1, accountDst);
        revelator.writeLongDataUnsafe(index + 2, amount);
        revelator.writeLongDataUnsafe(index + 3, ((long) currency << 8) | transferType.getCode());

        revelator.writeLongDataUnsafe(index + 4, sha256[0]);
        revelator.writeLongDataUnsafe(index + 5, sha256[1]);
        revelator.writeLongDataUnsafe(index + 6, sha256[2]);
        revelator.writeLongDataUnsafe(index + 7, sha256[3]);

        revelator.publish(claimSeq + msgSize);
    }

    public void openAccount(final long timestamp,
                            final long correlationId,
                            final long account,
                            final long secret) {

//        log.debug("OpenAcc >>> t={}", timestamp);

        final int msgSize = 2;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_OPEN_ACCOUNT);

//        log.debug("claimSeq={}", claimSeq);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, account);
        revelator.writeLongDataUnsafe(index + 1, secret);

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);

    }

    public void adjustBalance(final long timestamp,
                              final long correlationId,
                              final long account,
                              final long amount) {

//        log.debug("Adjust >>> t={}", timestamp);

        final int msgSize = 2;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_ADJUST_BALANCE);

//        log.debug("claimSeq={}", claimSeq);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, account);
        revelator.writeLongDataUnsafe(index + 1, amount);

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);

    }

    public void adjustCurrencyRate(final long timestamp,
                                   final long correlationId,
                                   final short currencyFrom,
                                   final short currencyTo,
                                   final double rate) {

//        log.debug("adjustCurrencyRate >>> t={}", timestamp);

        final int msgSize = 2;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_CTRL_CUR_RATE);

//        log.debug("claimSeq={}", claimSeq);

        final int index = (int) (claimSeq & indexMask);

        // TODO validate

        revelator.writeLongDataUnsafe(index, ((long) currencyFrom << 32) + currencyTo);
        revelator.writeLongDataUnsafe(index + 1, Double.doubleToLongBits(rate));

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);
    }

    public void adjustFee(final long timestamp,
                          final long correlationId,
                          final double feeK,
                          final Map<Short, FeeConfig> feeLimits) {

//        log.debug("adjustCurrencyRate >>> t={}", timestamp);

        final int msgSize = 1 + feeLimits.size() * 3; // TODO check size

        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_CTRL_FEES);

//        log.debug("claimSeq={}", claimSeq);

        final int index = (int) (claimSeq & indexMask);

        // TODO validate

        revelator.writeLongDataUnsafe(index, Double.doubleToLongBits(feeK));

        final MutableInteger offset = new MutableInteger(1);
        feeLimits.forEach((currency, feeConfig) -> {
            revelator.writeLongDataUnsafe(index + offset.value, currency);
            revelator.writeLongDataUnsafe(index + offset.value + 1, feeConfig.minFee);
            revelator.writeLongDataUnsafe(index + offset.value + 2, feeConfig.maxFee);
            offset.value += 3;
        });

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);
    }

    public static final record FeeConfig(long minFee, long maxFee) {
    }

    public void customQuery(final byte cmdCode,
                            final long timestamp,
                            final long correlationId,
                            final long data) {

//        log.debug("Custom >>> t={} cmdCode={}", timestamp, cmdCode);

        final int msgSize = 1;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, cmdCode);

//        log.debug("claimSeq={}", claimSeq);
        final int index = (int) (claimSeq & indexMask);
        revelator.writeLongDataUnsafe(index, data);

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);
    }

}
