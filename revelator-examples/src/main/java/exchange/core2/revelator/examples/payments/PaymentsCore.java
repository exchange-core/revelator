package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.buffers.LocalResultsLongBuffer;
import exchange.core2.revelator.fences.AggregatingMinFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.processors.pipelined.PipelinedFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleFlowProcessor;
import org.agrona.BitUtil;
import org.agrona.collections.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public final class PaymentsCore {

    private static final Logger log = LoggerFactory.getLogger(PaymentsCore.class);

    public final static int BUFFER_SIZE = 1024 * 1024;

    private final Revelator revelator;
    private final PaymentsApi paymentsApi;


    public static PaymentsCore createSimple(IPaymentsResponseHandler responseHandler,
                                            ThreadFactory threadFactory) {

        final LocalResultsByteBuffer resultsBuffer = LocalResultsByteBuffer.create(BUFFER_SIZE);

        final AccountsProcessor accountsProcessor = new AccountsProcessor();

        final SimplePaymentsHandler paymentsHandler = new SimplePaymentsHandler(accountsProcessor, resultsBuffer);

        final ResponsesAggregator responsesAggregator = new ResponsesAggregator(resultsBuffer, responseHandler);

        final IFlowProcessorsFactory processorsFactory = ProcessorsFactories.chain(
                List.of(paymentsHandler, responsesAggregator));

        final Revelator revelator = Revelator.create(
                BUFFER_SIZE,
                processorsFactory,
                threadFactory);

        final PaymentsApi paymentsApi = new PaymentsApi(revelator, revelator.getIndexMask());

        return new PaymentsCore(revelator, paymentsApi);
    }

    public static PaymentsCore createParallel(IPaymentsResponseHandler responseHandler,
                                              ThreadFactory threadFactory,
                                              int threadsNum) {
        if (!BitUtil.isPowerOfTwo(threadsNum)) {
            throw new IllegalArgumentException("Number of threads must be power of 2");
        }

        final long handlersMask = threadsNum - 1;

        final LocalResultsLongBuffer[] resultsBuffers = new LocalResultsLongBuffer[threadsNum];
        final IFence[] transferFences = new IFence[threadsNum];

        final IFlowProcessorsFactory processorsFactory = (inboundFence, config) -> {

            final List<IFlowProcessor> processors = new ArrayList<>();

            for (int i = 0; i < threadsNum; i++) {

                final LocalResultsLongBuffer resultsBuffer = LocalResultsLongBuffer.create(BUFFER_SIZE);
                resultsBuffers[i] = resultsBuffer;

                final AccountsProcessor accountsProcessor = new AccountsProcessor();

                final PaymentsHandlerParallel paymentsHandler = new PaymentsHandlerParallel(
                        accountsProcessor,
                        resultsBuffer,
                        i,
                        handlersMask);

                final SimpleFlowProcessor paymentsProcessor = new SimpleFlowProcessor(
                        paymentsHandler,
                        inboundFence,
                        config);

                transferFences[i] = paymentsProcessor.getReleasingFence();
                processors.add(paymentsProcessor);

            }

            final ResponsesSmartAggregator responsesAggregator = new ResponsesSmartAggregator(
                    resultsBuffers,
                    transferFences,
                    handlersMask,
                    responseHandler,
                    config.getBuffer());

            final SimpleFlowProcessor resultsProcessor = new SimpleFlowProcessor(
                    responsesAggregator,
                    inboundFence,
                    config);

            processors.add(resultsProcessor);

            return new IFlowProcessorsFactory.ProcessorsChain(
                    processors,
                    resultsProcessor.getReleasingFence());
        };

        final Revelator revelator = Revelator.create(
                BUFFER_SIZE,
                processorsFactory,
                threadFactory);

        final PaymentsApi paymentsApi = new PaymentsApi(revelator, revelator.getIndexMask());

        return new PaymentsCore(revelator, paymentsApi);
    }

    public static PaymentsCore createPipelined(IPaymentsResponseHandler responseHandler,
                                               ThreadFactory threadFactory,
                                               int threadsNum) {

        if (!BitUtil.isPowerOfTwo(threadsNum)) {
            throw new IllegalArgumentException("Number of threads must be power of 2");
        }

        final long handlersMask = threadsNum - 1;

        final LocalResultsLongBuffer[] resultsBuffers = new LocalResultsLongBuffer[threadsNum];
        final IFence[] fencesSt1 = new IFence[threadsNum];

        final IFlowProcessorsFactory processorsFactory = (inboundFence, config) -> {

            final List<IFlowProcessor> processors = new ArrayList<>();

            final List<IFence> outboundFences = new ArrayList<>();

            for (int i = 0; i < threadsNum; i++) {

                final LocalResultsLongBuffer resultsBuffer = LocalResultsLongBuffer.create(BUFFER_SIZE);
                resultsBuffers[i] = resultsBuffer;

                final SingleWriterFence fenceSt1 = new SingleWriterFence();
                fencesSt1[i] = fenceSt1;

                final LongHashSet lockedAccounts = new LongHashSet(20);

                final AccountsProcessor accountsProcessor = new AccountsProcessor();

                CurrencyRateProcessor currencyRateProcessor = new CurrencyRateProcessor();
                TransferFeesProcessor transferFeesProcessor = new TransferFeesProcessor(currencyRateProcessor, accountsProcessor);

                final SignatureHandler signatureHandler = new SignatureHandler();

                final PaymentsHandlerStage1 handlerSt1 = new PaymentsHandlerStage1(
                        accountsProcessor,
                        transferFeesProcessor,
                        signatureHandler,
                        config.getBuffer(),
                        resultsBuffer,
                        fenceSt1,
                        lockedAccounts,
                        i,
                        handlersMask);


                final PaymentsHandlerStage2 handlerSt2 = new PaymentsHandlerStage2(
                        accountsProcessor,
                        transferFeesProcessor,
                        resultsBuffers,
                        lockedAccounts,
                        fencesSt1,
                        i,
                        handlersMask);


                final PipelinedFlowProcessor<TransferSession> transferProcessor = new PipelinedFlowProcessor<>(
                        List.of(handlerSt1, handlerSt2),
                        TransferSession::new,
                        inboundFence,
                        config.getIndexMask(),
                        config.getBuffer());

                processors.add(transferProcessor);

                outboundFences.add(transferProcessor.getReleasingFence());
            }

            final ResponsesSmartAggregator responsesAggregator = new ResponsesSmartAggregator(
                    resultsBuffers,
                    fencesSt1,
                    handlersMask,
                    responseHandler,
                    config.getBuffer());

            final SimpleFlowProcessor resultsProcessor = new SimpleFlowProcessor(
                    responsesAggregator,
                    inboundFence,
                    config);

            processors.add(resultsProcessor);
            outboundFences.add(resultsProcessor.getReleasingFence());

            return new IFlowProcessorsFactory.ProcessorsChain(
                    processors,
                    new AggregatingMinFence(outboundFences));
        };

        final Revelator revelator = Revelator.create(
                BUFFER_SIZE,
                processorsFactory,
                threadFactory);

        final PaymentsApi paymentsApi = new PaymentsApi(revelator, revelator.getIndexMask());

        return new PaymentsCore(revelator, paymentsApi);

    }


    private PaymentsCore(Revelator revelator,
                         PaymentsApi paymentsApi) {

        this.revelator = revelator;
        this.paymentsApi = paymentsApi;
    }

    public void start() {

        revelator.start();
    }

    public void stop() {

        log.info("Stopping revelator...");
        revelator.stopAsync().join();
        log.info("Revelator stopped");
    }

    public PaymentsApi getPaymentsApi() {
        return paymentsApi;
    }
}
