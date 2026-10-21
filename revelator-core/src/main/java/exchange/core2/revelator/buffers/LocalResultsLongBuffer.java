package exchange.core2.revelator.buffers;

import org.agrona.BitUtil;

public final class LocalResultsLongBuffer {

    private final int size;
    private final long[] buffer;
    private final long indexMask;

    // TODO can make msg size=4 - longs, so buffer will more compact (>>2)

    // TODO another idea - idependen counters (need deterministic framework)

    public static LocalResultsLongBuffer create(int revelatorBufferSize) {

        if (!BitUtil.isPowerOfTwo(revelatorBufferSize)) {
            throw new IllegalArgumentException("buffer size must be 2^N");
        }

        final int bufferSize = revelatorBufferSize;

        return new LocalResultsLongBuffer(bufferSize, new long[bufferSize]);
    }

    private LocalResultsLongBuffer(int size, long[] buffer) {
        this.size = size;
        this.buffer = buffer;
        this.indexMask = size - 1;
    }

    public long get(long position) {
        return buffer[(int) ((position) & indexMask)];
    }

    public void set(long position, long value) {
        buffer[(int) ((position) & indexMask)] = value;
    }

}
