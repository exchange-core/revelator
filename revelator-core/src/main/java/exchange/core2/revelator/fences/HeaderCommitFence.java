package exchange.core2.revelator.fences;

import exchange.core2.revelator.Revelator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Publication without a shared cursor: the publisher writes the last header word of every message (the commit word)
 * with release semantics after the rest of the message, and readers scan commit words from their own position.
 * Readers wait on the lines they have to read anyway instead of a cursor line that every message rewrites.
 * <p>
 * The commit word holds the message position, so a word left from a previous lap never matches. It is XOR-ed with a
 * constant so that payload data left at that index (small numbers, positions) does not look like a commit either.
 * <p>
 * Stateless: one instance serves all readers, each passes its own position.
 */
public final class HeaderCommitFence implements IFence {

    public static final int COMMIT_OFFSET = Revelator.MSG_HEADER_SIZE - 1;

    private static final long COMMIT_MAGIC = 0x5EED_C0DE_D1CE_F00DL;
    private static final int MAX_SCAN = 64; // messages per call

    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);

    private final long[] buffer;
    private final int indexMask;
    private final int bufferSize;

    public HeaderCommitFence(long[] buffer, int indexMask) {
        this.buffer = buffer;
        this.indexMask = indexMask;
        this.bufferSize = indexMask + 1;
    }

    /**
     * Makes the message (or padding header) at the position visible to readers.
     */
    public static void commit(final long[] buffer, final int index, final long position) {
        LONGS.setRelease(buffer, index + COMMIT_OFFSET, (position + 1) ^ COMMIT_MAGIC);
    }

    /**
     * @return end of the committed messages that follow lastKnown (lastKnown if the next one is not committed yet)
     */
    @Override
    public long getAcquire(final long lastKnown) {
        long committed = lastKnown;
        long pos = lastKnown;
        for (int n = 0; n < MAX_SCAN; n++) {
            final int index = (int) (pos & indexMask);
            if (bufferSize - index < Revelator.MSG_HEADER_SIZE) {
                // no header fits before the end of the ring: implicit padding, confirmed only by a commit after it
                pos = (pos | indexMask) + 1;
                continue;
            }
            final long commit = (long) LONGS.getAcquire(buffer, index + COMMIT_OFFSET);
            if (commit != ((pos + 1) ^ COMMIT_MAGIC)) {
                break;
            }
            pos = buffer[index] == 0L
                    ? (pos | indexMask) + 1                                  // padding header: skip to the next lap
                    : pos + Revelator.MSG_HEADER_SIZE + buffer[index + 2];
            committed = pos;
        }
        return committed;
    }

    @Override
    public long getVolatile() {
        throw new UnsupportedOperationException("header-commit has no single value, use getAcquire(position)");
    }

    @Override
    public long getOpaque() {
        throw new UnsupportedOperationException("header-commit has no single value, use getAcquire(position)");
    }
}
