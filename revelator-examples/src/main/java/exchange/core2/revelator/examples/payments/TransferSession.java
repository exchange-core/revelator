package exchange.core2.revelator.examples.payments;

import exchange.core2.revelator.processors.pipelined.PipelinedFlowSession;

public final class TransferSession extends PipelinedFlowSession {

    public long accountSrc;
    public long accountDst;

    public boolean processSrc;
    public boolean processDst;

    public long amountSrc;
    public long amountDst;

    public long treasureAmountSrc;
    public long treasureAmountDst;

    public boolean localPartSucceeded;


    //    public short currency;


    @Override
    public String toString() {
        return "TransferSession{" +
                "accountSrc=" + accountSrc +
                ", accountDst=" + accountDst +
                ", processSrc=" + processSrc +
                ", processDst=" + processDst +
                ", globalOffset=" + globalOffset +
                ", bufferIndex=" + bufferIndex +
                ", payloadSize=" + payloadSize +
                ", messageType=" + messageType +
                ", timestamp=" + timestamp +
                ", correlationId=" + correlationId +
                ", userCookie=" + userCookie +
                '}';
    }
}
