package exchange.core2.revelator.buffers;

import org.agrona.BitUtil;

public final class LocalResultsByteBuffer {

    private final int size;
    private final byte[] buffer;
    private final long indexMask;

    // TODO can make msg size=4 - longs, so buffer will more compact (>>2)

    // TODO another idea - idependen counters (need deterministic framework)

    public static LocalResultsByteBuffer create(int revelatorBufferSize) {

        if (!BitUtil.isPowerOfTwo(revelatorBufferSize)) {
            throw new IllegalArgumentException("buffer size must be 2^N");
        }

        final int bufferSize = revelatorBufferSize >> 1; // min message size is 3

        return new LocalResultsByteBuffer(bufferSize, new byte[bufferSize]);
    }

    private LocalResultsByteBuffer(int size, byte[] buffer) {
        this.size = size;
        this.buffer = buffer;
        this.indexMask = size - 1;
    }

    public byte get(long position) {
        return buffer[(int) ((position >> 1) & indexMask)];
    }

    public void set(long position, byte value) {
        buffer[(int) ((position >> 1) & indexMask)] = value;
    }

}
