package exchange.core2.revelator.perf;

import exchange.core2.collections.affinity.CpuAffinity;

import java.util.concurrent.ThreadFactory;

/**
 * Pins the n-th created thread to cpus[n]. Both frameworks create one thread per stage in stage order, so a stage
 * lands on the same core whichever framework runs it (core-to-core latency differs between mesh tiles).
 */
public final class PinnedThreadFactory implements ThreadFactory {

    private final int[] cpus;
    private int next = 0;

    public PinnedThreadFactory(int[] cpus) {
        this.cpus = cpus;
    }

    @Override
    public synchronized Thread newThread(Runnable runnable) {
        if (next >= cpus.length) {
            throw new IllegalStateException("not enough CPUs for consumer threads: " + cpus.length);
        }
        final int cpu = cpus[next];
        final String name = "stage-" + next + "-cpu" + cpu;
        next++;
        final Thread thread = new Thread(() -> {
            try (CpuAffinity affinity = CpuAffinity.acquireCore(cpu)) {
                if (!affinity.isPinned()) {
                    System.out.println("WARN " + name + " is not pinned");
                }
                runnable.run();
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }
}
