package exchange.core2.revelator.fences;

import exchange.core2.revelator.FrameLayout;
import exchange.core2.revelator.Revelator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.security.SecureRandom;

/**
 * Publication without a shared cursor: the publisher writes the last header word of every message (the commit word)
 * with release semantics after the rest of the message, and readers scan commit words from their own position.
 * Readers wait on the lines they have to read anyway instead of a cursor line that every message rewrites.
 * <p>
 * The commit word is the message position XOR-ed with a random tag of this ring. A commit word left from a previous
 * lap holds another position and never matches. With variable-size frames the word at a reader's position may also
 * have been payload in the previous lap; it passes for a commit only if it equals position ^ tag, which payload
 * cannot aim at without knowing the tag: the chance is 2^-64 per stale word read. (Clearing the commit word of the
 * next frame before publishing makes it impossible, but costs a transfer of a line the readers have prefetched:
 * MULTICAST with 5 readers, aligned frames, batch 1 lost a third of its throughput.)
 * <p>
 * One instance serves all readers, each passes its own position.
 */
public final class HeaderCommitFence implements IFence {

    public static final int COMMIT_OFFSET = Revelator.MSG_HEADER_SIZE - 1;

    private static final int MAX_SCAN = 64; // messages per call

    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);

    private final long[] buffer;
    private final int indexMask;
    private final int bufferSize;
    private final FrameLayout frameLayout;
    private final long tag;

    public HeaderCommitFence(long[] buffer, int indexMask, FrameLayout frameLayout) {
        this(buffer, indexMask, frameLayout, new SecureRandom().nextLong());
    }

    /**
     * @param tag for a publisher outside of Revelator that has to produce the same commit words (the top bit is set,
     *            so a commit word is never 0, the initial content of the ring)
     */
    public HeaderCommitFence(long[] buffer, int indexMask, FrameLayout frameLayout, long tag) {
        this.buffer = buffer;
        this.indexMask = indexMask;
        this.bufferSize = indexMask + 1;
        this.frameLayout = frameLayout;
        this.tag = tag | Long.MIN_VALUE;
    }

    public long tag() {
        return tag;
    }

    /**
     * Makes the message (or padding header) at the position visible to readers.
     */
    public void commit(final int index, final long position) {
        LONGS.setRelease(buffer, index + COMMIT_OFFSET, position ^ tag);
    }

    /**
     * Acquire-reads the commit word of the message (or padding header) that starts at the position.
     */
    public boolean isCommitted(final int index, final long position) {
        return (long) LONGS.getAcquire(buffer, index + COMMIT_OFFSET) == (position ^ tag);
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
                // no header fits before the end of the ring: implicit padding, the next frame starts the next lap
                pos = frameLayout.nextLap(pos, indexMask);
                committed = pos;
                continue;
            }
            if (!isCommitted(index, pos)) {
                break;
            }
            pos = buffer[index] == 0L
                    ? frameLayout.nextLap(pos, indexMask)                    // padding header: skip to the next lap
                    : frameLayout.nextFrame(pos + Revelator.MSG_HEADER_SIZE + buffer[index + 2]);
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
