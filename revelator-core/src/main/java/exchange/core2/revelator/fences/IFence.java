package exchange.core2.revelator.fences;

/**
 * Monotonic availability in long-word positions. A bound is an exclusive completed/published
 * prefix. getAcquire establishes visibility of data before the returned bound. lastKnown is the
 * reader's current frame position (or cached bound), never a message ordinal. Implementations may
 * return a conservative bound. Mutable reader caches belong to one consumer and must not cross
 * epochs. Volatile/opaque numeric access is optional and is unsupported by legacy positional
 * scanners.
 */
public interface IFence {
    long getAcquire(long lastKnown);

    long getVolatile();

    long getOpaque();
}
