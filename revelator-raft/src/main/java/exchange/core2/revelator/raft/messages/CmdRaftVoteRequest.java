package exchange.core2.revelator.raft.messages;

import java.nio.ByteBuffer;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public record CmdRaftVoteRequest(int term, int candidateId, long lastLogIndex, int lastLogTerm) implements RpcRequest {

    @Override
    public int getMessageType() {
        return 2;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putInt(candidateId);
        buffer.putLong(lastLogIndex);
        buffer.putInt(lastLogTerm);
    }


    public static CmdRaftVoteRequest create(ByteBuffer buffer) {

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();

        return new CmdRaftVoteRequest(term, leaderId, prevLogIndex, prevLogTerm);
    }

    @Override
    public String toString() {
        return "CmdRaftVoteRequest{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
