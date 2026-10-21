package exchange.core2.revelator.processors.pipelined;

public class PipelinedFlowSession {

    public long globalOffset;
    public int bufferIndex;

    public int payloadSize;
    public byte messageType;
    public long timestamp;
    public long correlationId;
    public int userCookie;

//    public int wordsLeftInBatch;

}
