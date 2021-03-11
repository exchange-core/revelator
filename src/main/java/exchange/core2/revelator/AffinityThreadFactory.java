package exchange.core2.revelator;

import net.openhft.affinity.AffinityLock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class AffinityThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(AffinityThreadFactory.class);

    private static AtomicInteger threadsCounter = new AtomicInteger();

    private final ThreadAffinityMode threadAffinityMode;


    public AffinityThreadFactory(ThreadAffinityMode threadAffinityMode) {
        this.threadAffinityMode = threadAffinityMode;
    }

    @Override
    public synchronized Thread newThread(@NotNull Runnable runnable) {

        // log.info("---- Requesting thread for {}", runnable);

        if (threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_DISABLE) {
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        return new Thread(() -> executePinned(runnable));

    }

    private void executePinned(@NotNull Runnable runnable) {

        try (final AffinityLock lock = getAffinityLockSync()) {

            final int threadId = threadsCounter.incrementAndGet();
            Thread.currentThread().setName(String.format("Thread-AF-%d-cpu%d", threadId, lock.cpuId()));

            log.debug("{} will be running on thread={} pinned to cpu {}",
                    runnable, Thread.currentThread().getName(), lock.cpuId());

            runnable.run();
        }
    }

    private synchronized AffinityLock getAffinityLockSync() {
        return threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE
                ? AffinityLock.acquireCore()
                : AffinityLock.acquireLock();
    }

    public enum ThreadAffinityMode {
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE,
        THREAD_AFFINITY_DISABLE
    }

}
