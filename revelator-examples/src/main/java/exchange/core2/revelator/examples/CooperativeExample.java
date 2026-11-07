package exchange.core2.revelator.examples;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.pipelined.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Preparation stays private in a reusable session until an external decision permits application.
 */
public final class CooperativeExample {
    static final class Session extends PipelinedFlowSession {
        long prepared;

        @Override
        public void reset() {
            prepared = 0;
        }
    }

    public static void main(String[] args) {
        AtomicBoolean permitted = new AtomicBoolean();
        long[] total = {0};
        IFlowProcessorsFactory factory =
                (input, config) -> {
                    PipelinedStageHandler<Session> prepare =
                            session -> {
                                session.prepared = config.getBuffer()[session.bufferIndex] * 2;
                                return true;
                            };
                    PipelinedStageHandler<Session> apply =
                            session -> {
                                if (!permitted.get()) return false;
                                total[0] += session.prepared;
                                return true;
                            };
                    var processor =
                            new PipelinedFlowProcessor<>(
                                    List.of(prepare, apply),
                                    Session::new,
                                    input,
                                    config.getIndexMask(),
                                    config.getBuffer());
                    return new IFlowProcessorsFactory.ProcessorsChain(
                            List.of(processor), processor.getReleasingFence());
                };
        try (Revelator runtime = Revelator.create(1024, factory, Thread::new)) {
            runtime.start();
            for (int i = 1; i <= 10; i++) {
                long payload = runtime.claimSingleMessage(1, 0, i, (byte) 1);
                runtime.writeLongData(payload, 0, i);
                runtime.publish(payload + 1);
            }
            permitted.set(true);
        }
        System.out.println("total=" + total[0]);
    }
}
