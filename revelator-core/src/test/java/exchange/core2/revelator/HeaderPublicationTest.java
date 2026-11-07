package exchange.core2.revelator;

import exchange.core2.revelator.fences.HeaderCommitFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.processors.pipelined.PipelinedFlowProcessor;
import exchange.core2.revelator.processors.pipelined.PipelinedFlowSession;
import exchange.core2.revelator.processors.pipelined.PipelinedStageHandler;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class HeaderPublicationTest {

    @Test(timeout = 10000)
    public void variableFramesAndWrap() {
        for (FrameLayout layout : List.of(FrameLayout.packed(), FrameLayout.lineAligned())) {
            final long[] seen = {0};
            try (Revelator r = Revelator.create(1024, ProcessorsFactories.single((b, i, n, t, p, id, k) -> {
                assertEquals(seen[0], b[i]);
                for (int j = 1; j < n; j++) {
                    assertEquals(b[i] ^ j, b[i + j]);
                }
                seen[0]++;
            }), Thread::new, Revelator.PublicationMode.HEADER_COMMIT, layout)) {
                r.start();
                for (int v = 0; v < 10000; v++) {
                    final int n = 1 + (v * 97) % 500;
                    final long p = r.claimSingleMessage(n, 0, v, (byte) 1);
                    for (int j = 0; j < n; j++) {
                        r.writeLongData(p, j, j == 0 ? v : v ^ j);
                    }
                    r.publish(p + n);
                }
            }
            assertEquals(10000, seen[0]);
        }
    }

    @Test(timeout = 10000)
    public void batchReverseAndPrefix() {
        for (Revelator.PublicationMode mode : Revelator.PublicationMode.values()) {
            final AtomicInteger count = new AtomicInteger();
            try (Revelator r = Revelator.create(4096,
                    ProcessorsFactories.single((b, i, n, t, p, id, k) -> count.incrementAndGet()), Thread::new, mode)) {
                r.start();
                for (int batch = 0; batch < 100; batch++) {
                    long prefixEnd = 0;
                    long end = 0;
                    for (int i = 0; i < 16; i++) {
                        final long p = r.claimSingleMessage(1, 0, 1, (byte) 1);
                        r.writeLongData(p, 0, i);
                        end = p + 1;
                        if (i == 3) {
                            prefixEnd = end;
                        }
                    }
                    r.publish(prefixEnd);
                    r.publish(end);
                }
            }
            assertEquals(1600, count.get());
        }
    }

    /**
     * A payload word of the previous lap in the place of the next frame's commit word must not pass for a commit:
     * here it holds what the commit word would be with the old constant tag.
     */
    @Test(timeout = 5000)
    public void oldPayloadDoesNotPassForCommit() throws Exception {
        final IFence[] input = new IFence[1];
        final SingleWriterFence released = new SingleWriterFence();
        final AtomicReference<Thread> worker = new AtomicReference<>();
        final IFlowProcessor parked = new IFlowProcessor() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.onSpinWait();
                }
            }

            @Override
            public SingleWriterFence getReleasingFence() {
                return released;
            }
        };
        final Revelator r = Revelator.create(1024, (fence, config) -> {
            input[0] = fence;
            return new IFlowProcessorsFactory.ProcessorsChain(List.of(parked), released);
        }, job -> {
            final Thread t = new Thread(job);
            worker.set(t);
            return t;
        }, Revelator.PublicationMode.HEADER_COMMIT);
        try {
            r.start();
            // the first lap: a header at index 1028 & 1023 = 4 with the old-style commit word of position 1028
            long p = r.claimSingleMessage(1004, 0, 1, (byte) 1);
            r.writeLongData(p, 0, 1L << 56);
            r.writeLongData(p, 1, 44);
            r.writeLongData(p, 2, 0);
            r.writeLongData(p, 3, (1028L + 1) ^ 0x5EED_C0DE_D1CE_F00DL);
            r.publish(p + 1004);
            released.setRelease(p + 1004);
            for (int i = 0; i < 5; i++) {
                p = r.claimSingleMessage(0, 0, 1, (byte) 1);
                r.publish(p);
                released.setRelease(p);
            }
            assertEquals(1028, input[0].getAcquire(1028));
        } finally {
            r.abort(new RuntimeException("test finished"));
            worker.get().join(1000);
        }
    }

    @Test
    public void tagIsRandomAndNeverGivesZero() {
        final long[] buffer = new long[1024];
        final HeaderCommitFence a = new HeaderCommitFence(buffer, 1023, FrameLayout.packed());
        final HeaderCommitFence b = new HeaderCommitFence(buffer, 1023, FrameLayout.packed());
        assertNotEquals(a.tag(), b.tag());
        assertTrue(a.tag() < 0 && b.tag() < 0);
        assertFalse(a.isCommitted(0, 0));
        a.commit(0, 0);
        assertTrue(a.isCommitted(0, 0));
        assertFalse(b.isCommitted(0, 0));
    }

    @Test(timeout = 10000)
    public void fullSizeFrameAfterPaddingAndAfterExactEnd() {
        for (Revelator.PublicationMode mode : Revelator.PublicationMode.values()) {
            final AtomicInteger count = new AtomicInteger();
            try (Revelator r = Revelator.create(1024,
                    ProcessorsFactories.single((b, i, n, t, p, id, k) -> count.incrementAndGet()), Thread::new, mode)) {
                r.start();
                for (int i = 0; i < 100; i++) {
                    long p = r.claimSingleMessage(3, 0, 0, (byte) 1);
                    r.publish(p + 3);
                    p = r.claimSingleMessage(1004, 0, 0, (byte) 1);
                    r.publish(p + 1004);
                }
            }
            assertEquals(200, count.get());
        }
    }

    @Test(timeout = 10000)
    public void headerScannerSupportsCooperativePipeline() {
        final AtomicInteger count = new AtomicInteger();
        try (Revelator r = Revelator.create(1024, (fence, config) -> {
            final PipelinedStageHandler<PipelinedFlowSession> stage = s -> {
                count.incrementAndGet();
                return true;
            };
            final PipelinedFlowProcessor<PipelinedFlowSession> p = new PipelinedFlowProcessor<>(
                    List.of(stage), PipelinedFlowSession::new, fence, config.getIndexMask(), config.getBuffer());
            return new IFlowProcessorsFactory.ProcessorsChain(List.of(p), p.getReleasingFence());
        }, Thread::new, Revelator.PublicationMode.HEADER_COMMIT)) {
            r.start();
            for (int i = 0; i < 500; i++) {
                final long p = r.claimSingleMessage(17, 0, 0, (byte) 1);
                r.publish(p + 17);
            }
        }
        assertEquals(500, count.get());
    }
}
