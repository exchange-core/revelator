package exchange.core2.revelator.fences;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

// 128 bytes of padding on both sides of value: the adjacent-line prefetcher pulls cache lines in pairs
abstract class SingleWriterFenceLhsPad {
    long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15, p16;
}

abstract class SingleWriterFenceValue extends SingleWriterFenceLhsPad {
    protected volatile long value = -1;
}

abstract class SingleWriterFenceRhsPad extends SingleWriterFenceValue {
    long p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32;
}

public final class SingleWriterFence extends SingleWriterFenceRhsPad implements IFence {

    private final static Logger logger = LoggerFactory.getLogger(SingleWriterFence.class);



    private static final VarHandle VALUE;

    static {
        try {
            VALUE = MethodHandles.lookup().findVarHandle(SingleWriterFenceValue.class, "value", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Perform a acquire-read (RA-mode) of this sequence's value.
     *
     * @return The current value of the sequence.
     */
    @Override
    public long getAcquire(final long ignore) {
        return (long) VALUE.getAcquire(this);
    }

    @Override
    public long getVolatile() {
        return (long) VALUE.getVolatile(this);
    }

    @Override
    public long getOpaque() {
        return (long) VALUE.getOpaque(this);
    }

    /**
     * Perform an ordered write of this sequence.  The intent is
     * a Store/Store barrier between this write and any previous
     * store.
     *
     * @param value The new value for the sequence.
     */
    public void setRelease(final long value) {
        VALUE.setRelease(this, value);
    }

    /**
     * Performs a volatile write of this sequence.  The intent is
     * a Store/Store barrier between this write and any previous
     * write and a Store/Load barrier between this write and any
     * subsequent volatile read.
     *
     * @param value The new value for the sequence.
     */
    public void setVolatile(final long value) {
        VALUE.setVolatile(this, value);
    }


}
