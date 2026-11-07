package exchange.core2.revelator;

import static org.junit.Assert.*;

import exchange.core2.revelator.processors.*;
import exchange.core2.revelator.processors.pipelined.*;
import exchange.core2.revelator.processors.reordering.*;

import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RuntimeTest {
    private static void rejected(Runnable job) {
        try {
            job.run();
            fail("expected rejection");
        } catch (IllegalArgumentException
                | IllegalStateException
                | UnsupportedOperationException expected) {
        }
    }

    @Test(timeout = 10000)
    public void cursorWrapAndClose() {
        long[] total = {0};
        int[] count = {0};
        Revelator r =
                Revelator.create(
                        1024,
                        ProcessorsFactories.single(
                                (b, i, n, t, p, id, k) -> {
                                    assertEquals(count[0], b[i]);
                                    total[0] += b[i];
                                    count[0]++;
                                }),
                        Thread::new);
        r.start();
        for (int j = 0; j < 20000; j++) {
            int size = 1 + j % 29;
            long p = r.claimSingleMessage(size, 0, j, (byte) 1);
            r.writeLongData(p, 0, j);
            r.publish(p + size);
        }
        r.close();
        assertEquals(20000, count[0]);
        assertEquals(19999L * 20000 / 2, total[0]);
        assertEquals(Revelator.State.TERMINATED, r.state());
        rejected(() -> r.claimSingleMessage(0, 0, 0, (byte) 1));
        rejected(r::start);
        r.close();
    }

    @Test(timeout = 5000)
    public void badInputAndUnpublishedShutdown() {
        Revelator r =
                Revelator.create(
                        1024,
                        ProcessorsFactories.single((b, i, n, t, p, id, k) -> {}),
                        Thread::new);
        r.start();
        rejected(() -> r.claimSingleMessage(Integer.MAX_VALUE, 0, 0, (byte) 1));
        rejected(() -> r.claimSingleMessage(0, 0, 0, (byte) 31));
        long p = r.claimSingleMessage(1, 0, 0, (byte) 1);
        rejected(() -> r.publish(p + 2));
        rejected(r::stopAsync);
        r.publish(p + 1);
        r.close();
        rejected(() -> ProcessorsFactories.chain(List.of()));
    }

    @Test(timeout = 5000)
    public void failStopSurfacesAndRejectsProducer() throws Exception {
        CountDownLatch attempted = new CountDownLatch(1);
        Revelator r =
                Revelator.create(
                        1024,
                        ProcessorsFactories.single(
                                (b, i, n, t, p, id, k) -> {
                                    attempted.countDown();
                                    throw new IllegalArgumentException("business failure");
                                }),
                        Thread::new);
        r.start();
        long p = r.claimSingleMessage(0, 0, 0, (byte) 1);
        r.publish(p);
        assertTrue(attempted.await(1, TimeUnit.SECONDS));
        try {
            r.stopAsync(Duration.ofSeconds(1)).get(2, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException expected) {
        }
        assertEquals(Revelator.State.FAILED, r.state());
        assertNotNull(r.failure());
        rejected(() -> r.claimSingleMessage(0, 0, 0, (byte) 1));
    }

    @Test(timeout = 5000)
    public void fullRingShutdownHasDeadline() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), finish = new CountDownLatch(1);
        Revelator r =
                Revelator.create(
                        1024,
                        ProcessorsFactories.single(
                                (b, i, n, t, p, id, k) -> {
                                    entered.countDown();
                                    while (finish.getCount() != 0) {
                                        try {
                                            finish.await();
                                        } catch (InterruptedException ignored) {
                                        }
                                    }
                                }),
                        job -> {
                            Thread t = new Thread(job);
                            t.setDaemon(true);
                            return t;
                        });
        r.start();
        long p = r.claimSingleMessage(1012, 0, 0, (byte) 1);
        r.publish(p + 1012);
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        p = r.claimSingleMessage(0, 0, 0, (byte) 1);
        r.publish(p);
        p = r.claimSingleMessage(0, 0, 0, (byte) 1);
        r.publish(p);
        long start = System.nanoTime();
        var stopped = r.stopAsync(Duration.ofMillis(100));
        assertTrue(System.nanoTime() - start < 500_000_000);
        try {
            stopped.get(2, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException expected) {
        } finally {
            finish.countDown();
        }
        assertEquals(Revelator.State.FAILED, r.state());
    }

    static final class Session extends PipelinedFlowSession {
        long value;

        @Override
        public void reset() {
            value = -1;
        }
    }

    @Test(timeout = 10000)
    public void cooperativeProgressAndShutdown() throws Exception {
        AtomicBoolean allowLast = new AtomicBoolean();
        CountDownLatch prefix = new CountDownLatch(99);
        int[] shutdown = {0};
        long[] sum = {0};
        AtomicReference<PipelinedFlowProcessor<Session>> proc = new AtomicReference<>();
        Revelator r =
                Revelator.create(
                        2048,
                        (input, config) -> {
                            PipelinedStageHandler<Session> a =
                                    s -> {
                                        assertEquals(-1, s.value);
                                        s.value = config.getBuffer()[s.bufferIndex];
                                        assertEquals(s.endExclusive, s.frameStart + 5);
                                        return true;
                                    };
                            PipelinedStageHandler<Session> b =
                                    new PipelinedStageHandler<>() {
                                        public boolean process(Session s) {
                                            if (s.value == 99 && !allowLast.get()) return false;
                                            sum[0] += s.value;
                                            prefix.countDown();
                                            return true;
                                        }

                                        public void onShutdown() {
                                            shutdown[0]++;
                                        }
                                    };
                            var pipeline =
                                    new PipelinedFlowProcessor<>(
                                            List.of(a, b),
                                            Session::new,
                                            input,
                                            config.getIndexMask(),
                                            config.getBuffer());
                            proc.set(pipeline);
                            return new IFlowProcessorsFactory.ProcessorsChain(
                                    List.of(pipeline), pipeline.getReleasingFence());
                        },
                        Thread::new);
        r.start();
        for (int j = 0; j < 100; j++) {
            long p = r.claimSingleMessage(1, 0, j, (byte) 1);
            r.writeLongData(p, 0, j);
            r.publish(p + 1);
        }
        assertTrue(prefix.await(2, TimeUnit.SECONDS));
        assertTrue(proc.get().getReleasingFence().getAcquire(0) > 0);
        allowLast.set(true);
        r.close();
        assertEquals(4950, sum[0]);
        assertEquals(1, shutdown[0]);
    }

    @Test(timeout = 5000)
    public void globalReorderingBarrierOrdersZeroSignature() throws Exception {
        AtomicBoolean ready = new AtomicBoolean();
        CountDownLatch pending = new CountDownLatch(1);
        CountDownLatch secondObserved = new CountDownLatch(1);
        StringBuilder order = new StringBuilder();
        ReorderingMessageHandler h =
                new ReorderingMessageHandler() {
                    public long signature(long[] b, int i, int n, byte type) {
                        if (b[i] == 2) secondObserved.countDown();
                        return b[i] == 1 ? 0 : -1L;
                    }

                    public boolean tryHandle(
                            long[] b, int i, int n, long ts, long pos, long id, byte type) {
                        if (b[i] == 1 && !ready.get()) {
                            pending.countDown();
                            return false;
                        }
                        order.append(b[i]);
                        return true;
                    }
                };
        Revelator r =
                Revelator.create(
                        1024,
                        (input, c) -> {
                            var p = new ReorderingFlowProcessor(h, input, c, 64);
                            return new IFlowProcessorsFactory.ProcessorsChain(
                                    List.of(p), p.getReleasingFence());
                        },
                        Thread::new);
        r.start();
        for (int j = 1; j <= 2; j++) {
            long p = r.claimSingleMessage(1, 0, j, (byte) 1);
            r.writeLongData(p, 0, j);
            r.publish(p + 1);
        }
        assertTrue(pending.await(1, TimeUnit.SECONDS));
        assertTrue(secondObserved.await(1, TimeUnit.SECONDS));
        ready.set(true);
        r.close();
        assertEquals("12", order.toString());
    }

    @Test(timeout = 5000)
    public void globalPendingBarrierBlocksLaterZeroSignature() throws Exception {
        AtomicBoolean ready = new AtomicBoolean();
        CountDownLatch pending = new CountDownLatch(1);
        CountDownLatch secondObserved = new CountDownLatch(1);
        StringBuilder order = new StringBuilder();
        ReorderingMessageHandler h =
                new ReorderingMessageHandler() {
                    public long signature(long[] b, int i, int n, byte type) {
                        if (b[i] == 2) secondObserved.countDown();
                        return b[i] == 1 ? -1L : 0;
                    }

                    public boolean tryHandle(
                            long[] b, int i, int n, long ts, long pos, long id, byte type) {
                        if (b[i] == 1 && !ready.get()) {
                            pending.countDown();
                            return false;
                        }
                        order.append(b[i]);
                        return true;
                    }
                };
        Revelator r =
                Revelator.create(
                        1024,
                        (input, c) -> {
                            var p = new ReorderingFlowProcessor(h, input, c, 64);
                            return new IFlowProcessorsFactory.ProcessorsChain(
                                    List.of(p), p.getReleasingFence());
                        },
                        Thread::new);
        r.start();
        for (int j = 1; j <= 2; j++) {
            long p = r.claimSingleMessage(1, 0, j, (byte) 1);
            r.writeLongData(p, 0, j);
            r.publish(p + 1);
        }
        assertTrue(pending.await(1, TimeUnit.SECONDS));
        assertTrue(secondObserved.await(1, TimeUnit.SECONDS));
        ready.set(true);
        r.close();
        assertEquals("12", order.toString());
    }

    @Test
    public void closeBeforeStart() {
        Revelator r =
                Revelator.create(
                        1024,
                        ProcessorsFactories.single((b, i, n, t, p, id, k) -> {}),
                        Thread::new);
        r.close();
        assertEquals(Revelator.State.TERMINATED, r.state());
        rejected(r::start);
    }
}
