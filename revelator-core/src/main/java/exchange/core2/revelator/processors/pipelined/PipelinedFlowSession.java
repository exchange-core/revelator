package exchange.core2.revelator.processors.pipelined;

public class PipelinedFlowSession {

    /** Frame beginning, in long words. */
    public long frameStart;

    /** Payload end, exclusive, in long words. */
    public long endExclusive;

    public int bufferIndex;

    public int payloadSize;
    public byte messageType;
    public long timestamp;
    public long correlationId;
    public int userCookie;

    /** Override to clear application fields before each reuse. Do not modify scheduler metadata. */
    public void reset() {}
}
