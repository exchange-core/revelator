package exchange.core2.revelator.raft.messages;

import java.nio.ByteBuffer;

/**
 * Invoked by candidates to gather votes (5.2).
 */
public record CmdRaftVoteResponse(int term, boolean voteGranted) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RpcResponse.RESPONSE_VOTE;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.put(voteGranted ? (byte) 1 : (byte) 0);
    }

    public static CmdRaftVoteResponse create(ByteBuffer buffer) {

        final int term = buffer.getInt();
        final boolean voteGranted = buffer.get() == 1;

        return new CmdRaftVoteResponse(term, voteGranted);
    }

    @Override
    public String toString() {
        return "CmdRaftVoteResponse{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
