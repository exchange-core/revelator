package exchange.core2.revelator.processors;

import exchange.core2.revelator.fences.AggregatingMinFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.simple.SimpleFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessorsFactories {

    public static IFlowProcessorsFactory single(final SimpleMessageHandler handler) {

        return (inboundFence, config) -> {
            final SimpleFlowProcessor simpleFlowProcessor = new SimpleFlowProcessor(
                    handler,
                    inboundFence,
                    config);

            return new IFlowProcessorsFactory.ProcessorsChain(
                    List.of(simpleFlowProcessor),
                    simpleFlowProcessor.getReleasingFence());
        };
    }

    public static IFlowProcessorsFactory parallel(final Collection<SimpleMessageHandler> handlers) {

        return (inboundFence, config) -> {

            // preparing processors
            final List<IFlowProcessor> processors = handlers.stream()
                    .map(handler -> new SimpleFlowProcessor(
                            handler,
                            inboundFence,
                            config))
                    .collect(Collectors.toList());

            // preparing releasing fences
            final IFence[] releasingFences = processors.stream()
                    .map(IFlowProcessor::getReleasingFence)
                    .toArray(IFence[]::new);

            return new IFlowProcessorsFactory.ProcessorsChain(
                    processors,
                    new AggregatingMinFence(releasingFences));
        };
    }

    public static IFlowProcessorsFactory chain(final List<SimpleMessageHandler> handlers) {

        return (inboundFence, config) -> {

            final List<IFlowProcessor> processors = new ArrayList<>();
            IFence lastFence = inboundFence;

            for (SimpleMessageHandler handler : handlers) {

                final SimpleFlowProcessor processor = new SimpleFlowProcessor(
                        handler,
                        lastFence,
                        config);

                processors.add(processor);

                lastFence = processor.getReleasingFence();
            }

            return new IFlowProcessorsFactory.ProcessorsChain(
                    processors,
                    lastFence);
        };
    }

}
