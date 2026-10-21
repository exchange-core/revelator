package exchange.core2.revelator.examples.payments;

public final class ArithmeticUtils {


    public static long reciprocalMul(final long amount,
                                     final byte orderBefore,
                                     final int multiplier,
                                     final byte orderAfter) {

        return ((amount >> orderBefore) * multiplier) >> orderAfter;
    }

    public static long multiplyByRate(final long amount, final double rate) {

        final double resultDouble = amount * rate;

        if (resultDouble > DOUBLE_TO_LONG_MAX || resultDouble < DOUBLE_TO_LONG_MIN) {
            throw new ArithmeticException("Can not fit into long: " + amount + " * " + rate);
        } else {
            return (long) resultDouble;
        }
    }


    private final static double DOUBLE_TO_LONG_MAX = (double) Long.MAX_VALUE;
    private final static double DOUBLE_TO_LONG_MIN = (double) Long.MIN_VALUE;

}
