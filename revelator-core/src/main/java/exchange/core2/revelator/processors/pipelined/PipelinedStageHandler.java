package exchange.core2.revelator.processors.pipelined;

public interface PipelinedStageHandler<S extends PipelinedFlowSession> {

    /**
     * Process single message
     *
     * @return true if processing succeeded, false to retry later
     */
    boolean process(S messageSession);


    int getHitWorkWeight();

}
