package exchange.core2.revelator.utils;

import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class LatencyTools {

    private static final double[] PERCENTILES = new double[]{50, 90, 95, 99, 99.9, 99.99};

    public static Map<String, String> createLatencyReportFast(Histogram histogram) {
        final Map<String, String> fmt = new LinkedHashMap<>();
        Arrays.stream(PERCENTILES).forEach(p -> fmt.put(p + "%", formatNanos(histogram.getValueAtPercentile(p))));
        fmt.put("W", formatNanos(histogram.getMaxValue()));
        return fmt;
    }

    public static String formatNanos(long ns) {
        float value = ns;
        String timeUnit = "ns";

        if (value > 1000) {
            value /= 1000;
            timeUnit = "µs";
        }

        if (value > 1000) {
            value /= 1000;
            timeUnit = "ms";
        }

        if (value > 1000) {
            value /= 1000;
            timeUnit = "s";
        }

        if (value < 0.3) {
            return value + timeUnit;
        } else if (value < 3) {
            return Math.round(value * 100) / 100f + timeUnit;
        } else if (value < 30) {
            return Math.round(value * 10) / 10f + timeUnit;
        } else {
            return Math.round(value) + timeUnit;
        }
    }

}
