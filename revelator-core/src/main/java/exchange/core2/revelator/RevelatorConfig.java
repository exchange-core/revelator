package exchange.core2.revelator;

public class RevelatorConfig {

    private final int indexMask;
    private final int bufferSize;
    private final long[] buffer;

    public RevelatorConfig(final int indexMask,
                           final int bufferSize,
                           final long[] buffer) {

        this.indexMask = indexMask;
        this.bufferSize = bufferSize;
        this.buffer = buffer;
    }

    public int getIndexMask() {
        return indexMask;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long[] getBuffer() {
        return buffer;
    }
}
