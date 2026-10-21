package exchange.core2.revelator.processors;

import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.IFence;

import java.util.List;

public interface IFlowProcessorsFactory {

    ProcessorsChain createProcessors(
            IFence inboundFence,
            RevelatorConfig config);


    class ProcessorsChain {

        private final List<IFlowProcessor> processors;
        private final IFence releasingFence;

        public ProcessorsChain(List<IFlowProcessor> processors, IFence releasingFence) {
            this.processors = processors;
            this.releasingFence = releasingFence;
        }

        public List<IFlowProcessor> getProcessors() {
            return processors;
        }

        public IFence getReleasingFence() {
            return releasingFence;
        }
    }
}
