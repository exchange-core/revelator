package exchange.core2.revelator;

/**
 * Where frames (header + payload) start in the ring.
 * <p>
 * PACKED: each frame right after the previous one.
 * <p>
 * LINE_ALIGNED: the 4-long header fills the last 32 bytes of a cache line and the payload starts on the next line;
 * frames are rounded up to whole lines (an 80-byte message takes 128). Meant for HEADER_COMMIT: readers spin on the
 * header line of the next message, and with a packed layout the payload written into that same line costs a second
 * transfer of the line per message.
 * <p>
 * Java does not expose array addresses, so the layout assumes where cache lines start inside the ring: G1 places
 * arrays of half a region or more (humongous) at a region boundary, and with a 16-byte array header element 0 sits 16
 * bytes into a line, so lines start at elements 6, 14, 22, ... (linePhase 6). Set -Drevelator.linePhase=0..7 for a
 * different GC or array header size.
 */
public final class FrameLayout {

    private static final FrameLayout PACKED = new FrameLayout(-1);

    // frame starts satisfy (position & 7) == align; -1 = packed
    private final int align;

    private FrameLayout(int align) {
        this.align = align;
    }

    public static FrameLayout packed() {
        return PACKED;
    }

    public static FrameLayout lineAligned() {
        return lineAligned(Integer.getInteger("revelator.linePhase", 6));
    }

    /**
     * @param linePhase ring element (0..7) at which cache lines start
     */
    public static FrameLayout lineAligned(int linePhase) {
        if (linePhase < 0 || linePhase > 7) {
            throw new IllegalArgumentException("linePhase must be 0..7: " + linePhase);
        }
        return new FrameLayout((linePhase - Revelator.MSG_HEADER_SIZE) & 7);
    }

    public boolean isAligned() {
        return align >= 0;
    }

    /**
     * @return start of the frame that follows a frame ending at frameEnd
     */
    public long nextFrame(final long frameEnd) {
        return align < 0 ? frameEnd : frameEnd + ((align - frameEnd) & 7);
    }

    /**
     * @return start of the first frame of the next lap after position
     */
    public long nextLap(final long position, final int indexMask) {
        return nextFrame((position | indexMask) + 1);
    }

    @Override
    public String toString() {
        return align < 0 ? "PACKED" : "LINE_ALIGNED(headers at " + align + " mod 8)";
    }
}
