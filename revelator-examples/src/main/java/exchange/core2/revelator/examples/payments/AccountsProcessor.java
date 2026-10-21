package exchange.core2.revelator.examples.payments;

import org.agrona.collections.Hashing;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AccountsProcessor {

    private final LongLongHashMap balances = new LongLongHashMap();
    private final LongLongHashMap secrets = new LongLongHashMap();

    private static final Logger log = LoggerFactory.getLogger(AccountsProcessor.class);

    @Deprecated
    public boolean transfer(final long accountFrom,
                            final long accountTo,
                            final long amount) {
        try {
            // TODO currency rate and fees

            // find first account and check NSF
            final long availableFrom = balances.get(accountFrom);
            final long fromNewBalance = Math.subtractExact(availableFrom, amount);

//                log.debug("TRANSFER {} available={} amountSubstract={}", accountFrom, availableFrom, amount);

            if (fromNewBalance < 0) {
                log.debug("NSF accountFrom={} available={} amount={}", accountFrom, availableFrom, amount);
                return false; // NSF
            }

            // find second account
            final long balanceTo = balances.get(accountTo);
            final long toNewBalance = Math.addExact(balanceTo, amount);

            // updated both accounts balances
            balances.put(accountFrom, fromNewBalance);
            balances.put(accountTo, toNewBalance); // TODO check if exists

            return true;

        } catch (final ArithmeticException ex) {
            log.debug("Overflow accountFrom={} accountTo={}", accountFrom, accountTo);
            return false; // overflow
        }
    }

    @Deprecated
    public boolean adjustBalance(final long account, final long amount) {

        try {
            final long available = balances.get(account);
//            long available = balances2.get(account);
//            if (available == balances2.missingValue()) {
//                available = 0;
//            }


            final long newBalance = (amount <= 0)
                    ? Math.subtractExact(available, amount)
                    : Math.addExact(available, amount);

//            balances.put(account, newBalance);
            balances.put(account, newBalance);

            return true;

        } catch (final ArithmeticException ex) {

            return false; // overflow
        }
    }

    // unsafe
    public boolean withdrawal(final long account, final long amount) {

//            long b = -1 - balances.get(account);
//            log.debug("WITHDRAWAL {} raw={} bal={} amount={}", account, balances.get(account), b, amount);

        // decrement
        final long newBalance = balances.addToValue(account, amount);

        // should stay negative (-1 = 0)
        if (newBalance >= 0) {

            log.debug("withdrawal (WO) failed - NSF account={} amount={} resultingBalance={}", account, amount, -1 - balances.get(account));

            // revert
            balances.addToValue(account, -amount);
            return false;

        } else {
            return true;
        }

    }

    // unsafe
    public boolean deposit(final long account, final long amount) {

//            long b = -1 - balances.get(account);
//            log.debug("DEPOSIT {} raw={} bal={} amount={}", account, balances.get(account), b, amount);

        final long newEncodedBalance = balances.addToValue(account, -amount);

        if (isNegativeOrRemoved(newEncodedBalance)) {

            long b = -1 - balances.get(account);
            final String errMsg = String.format("Unsafe operation: DEPOSIT (DO) account=%d  amount=%d encodedBalance=%d balance=%d", account, amount, balances.get(account), b);
            throw new IllegalArgumentException(errMsg);
        }

        // if previous value was 0 - account did not exist
        if (newEncodedBalance == -amount) {

            log.debug("deposit (DO) failed - unknown account {}", account);

            // revert change
            balances.remove(account);
            return false;

        } else {
            return true;
        }
    }


    public void balanceCorrection(final long account, final long amount) {

//            long b = -1 - balances.get(account);
//            log.debug("CORRECTION {} raw={} bal={} amount={}", account, balances.get(account), b, amount);

        final long after = balances.addToValue(account, -amount);

        if (isNegativeOrRemoved(after)) {
            long b = -1 - after;
            final String errMsg = String.format("Unsafe operation: CORR account=%d  amount=%d encodedBalance=%d balance=%d", account, amount, after, b);
            throw new IllegalArgumentException(errMsg);
        }
    }

    // unsafe
    @Deprecated
    public boolean transferLocally(final long accountSrc,
                                   final long accountDst,
                                   final long amountSrc,
                                   final long amountDst) {

//            long b = -1 - balances.get(accountSrc);
//            long b1 = -1 - balances.get(accountDst);
//            log.debug("TRANSFER {}->{} rawSrc={} rawDst={} balSrc={} balDst={} amountSrc={} amountDst={}",
//                    accountSrc, accountDst, balances.get(accountSrc), balances.get(accountDst), b, b1, amountSrc, amountDst);

        // decrement source account balance
        final long newBalanceSrc = balances.addToValue(accountSrc, amountSrc);

        // should stay negative (-1 value = 0 balance)
        if (newBalanceSrc >= 0) {

            log.debug("withdrawal (TL) failed - NSF account={} amount={} resultingBalance={}", accountSrc, amountSrc, -1 - balances.get(accountSrc));

            // revert
            balances.addToValue(accountSrc, -amountSrc);
            return false;
        }

        final long newEncodedBalanceDst = balances.addToValue(accountDst, -amountDst);

        if (isNegativeOrRemoved(newEncodedBalanceDst)) {

            long b = -1 - balances.get(accountDst);
            final String errMsg = String.format("Unsafe operation: DEPOSIT (DO) account=%d  amount=%d encodedBalance=%d balance=%d",
                    accountDst, amountDst, balances.get(accountDst), b);

            throw new IllegalArgumentException(errMsg);
        }


        // if previous value was 0 - account did not exist
        if (newEncodedBalanceDst == -amountDst) {

            log.debug("deposit (TL) failed - unknown account {}", accountDst);

            // revert balance change
            balances.remove(accountDst);

            // revert source balance change
            balances.addToValue(accountSrc, -amountSrc);
            return false;
        }

        return true;
    }

    public void openNewAccount(final long account, final long secret) {
        balances.put(account, -1);
        secrets.put(account, secret);
    }

    public long getSecret(final long account){
        // assume 0L is valid secret - just don't let clients using it
        return secrets.get(account);
    }

    public boolean accountExists(final long account) {
        return balances.get(account) != 0;
    }

    public boolean accountNotExists(final long account) {
        return balances.get(account) == 0;
    }

    public boolean accountHasZeroBalance(final long account) {
        return balances.get(account) == -1;
    }

    public boolean isNegativeOrRemoved(final long encodedAmount) {
        return encodedAmount >= 0;
    }

    public void closeAccount(final long account) {
        balances.remove(account);
    }

    public long getBalance(final long account) {

        // not balance yet
        final long value = balances.get(account);
        if (value == 0) {
            throw new RuntimeException("Account does not exist");
        }

        return -1 - value;
    }


    public static long mapToAccount(long clientId, int currencyId, int accountNum) {

        if (clientId > 0x7_FFFF_FFFFL) {
            throw new IllegalArgumentException("clientId is too big");
        }

        if (currencyId > 0xFFFF) {
            throw new IllegalArgumentException("currencyId is too big");
        }

        if (accountNum > 0xFF) {
            throw new IllegalArgumentException("accountNum is too big");
        }

        final long accountRaw = (clientId << 28) | ((long) currencyId << 12) | ((long) accountNum << 4);
        final int checkDigit = Hashing.hash(accountRaw) & 0xF;
//        log.debug("{} {} {} -> {} + CD={} -> {}", clientId, currencyId, accountNum, accountRaw, checkDigit, accountRaw | checkDigit);
        return accountRaw | checkDigit;
    }

    public static short extractCurrency(long accountId) {

        return (short) (accountId >> 12);

    }

}
