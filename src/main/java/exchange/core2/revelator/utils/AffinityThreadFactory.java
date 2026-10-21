package exchange.core2.revelator.utils;

import exchange.core2.collections.affinity.CpuAffinity;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public final class AffinityThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(AffinityThreadFactory.class);


    private final ThreadAffinityMode threadAffinityMode;


    public AffinityThreadFactory(ThreadAffinityMode threadAffinityMode) {
        this.threadAffinityMode = threadAffinityMode;
    }

    @Override
    public synchronized Thread newThread(@NotNull Runnable runnable) {

        // log.info("---- Requesting thread for {}", runnable);

        if (threadAffinityMode == ThreadAffinityMode.NO_AFFINITY) {
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        return new Thread(() -> executePinned(runnable));

    }

    private void executePinned(@NotNull Runnable runnable) {

        try (final CpuAffinity lock = getAffinityLockSync()) {

            final Thread thread = Thread.currentThread();

            final String newName = String.format("%s-cpu%d", thread.getName(), lock.cpu());
            thread.setName(newName);

            log.debug("{} will be running on thread={} pinned to cpu {} ({})", runnable, newName, lock.cpu(), threadAffinityMode);

            runnable.run();
        }
    }

    // CpuAffinity always reserves the whole physical core, so both modes behave the same
    private synchronized CpuAffinity getAffinityLockSync() {
        return CpuAffinity.acquireCore();
    }

    public enum ThreadAffinityMode {
        AFFINITY_PHYSICAL_CORE,
        AFFINITY_LOGICAL_CORE,
        NO_AFFINITY
    }

}
