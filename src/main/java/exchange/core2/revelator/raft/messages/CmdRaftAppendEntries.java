package exchange.core2.revelator.raft.messages;

import exchange.core2.revelator.raft.RsmRequestFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public record CmdRaftAppendEntries<T extends RsmRequest>(int term,
                                                         int leaderId,
                                                         long prevLogIndex,
                                                         int prevLogTerm,
                                                         List<RaftLogEntry<T>> entries,
                                                         long leaderCommit) implements RpcRequest {

    @Override
    public int getMessageType() {
        return 1;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putInt(leaderId);
        buffer.putLong(prevLogIndex);
        buffer.putInt(prevLogTerm);
        buffer.putInt(entries.size());
        entries.forEach(entry -> entry.serialize(buffer));
        buffer.putLong(leaderCommit);
    }

    public static <T extends RsmRequest> CmdRaftAppendEntries<T> create(
            ByteBuffer buffer,
            RsmRequestFactory<T> factory) {

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();
        final int numEntries = buffer.getInt();

        final List<RaftLogEntry<T>> entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {

            entries.add(RaftLogEntry.create(buffer, factory));
        }

        final long leaderCommit = buffer.getLong();

        return new CmdRaftAppendEntries<>(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public String toString() {
        return "CmdRaftAppendEntries{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + entries +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
