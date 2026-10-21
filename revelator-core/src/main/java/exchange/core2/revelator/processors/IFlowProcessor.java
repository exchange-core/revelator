package exchange.core2.revelator.processors;

import exchange.core2.revelator.fences.SingleWriterFence;

public interface IFlowProcessor extends Runnable {

    SingleWriterFence getReleasingFence();

}
