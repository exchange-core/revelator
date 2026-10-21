package exchange.core2.revelator.raft.messages;

import java.nio.ByteBuffer;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 * <p>
 * TODO If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs. For example,
 * when rejecting an AppendEntries request, the follower can include the term of the conflicting entry and the first
 * index it stores for that term. With this information, the leader can decrement nextIndex to bypass all of the
 * conflicting entries in that term; one AppendEntries RPC will be required for each term with conflicting entries, rather
 * than one RPC per entry. In practice, we doubt this optimization is necessary, since failures happen infrequently
 * and it is unlikely that there will be many inconsistent entries.
 */
public record CmdRaftAppendEntriesResponse(int term, boolean success) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RpcResponse.RESPONSE_APPEND_ENTRIES;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.put(success ? (byte) 1 : (byte) 0);
    }

    public static CmdRaftAppendEntriesResponse create(ByteBuffer bb) {
        final int term = bb.getInt();
        final boolean success = bb.get() == 1;
        return new CmdRaftAppendEntriesResponse(term, success);
    }

    @Override
    public String toString() {
        return "CmdRaftAppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                '}';
    }
}
