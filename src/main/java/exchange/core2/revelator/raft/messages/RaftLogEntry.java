package exchange.core2.revelator.raft.messages;

import exchange.core2.revelator.raft.RsmRequestFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * each entry contains command for state machine, and term when entry was received by leader
 */
public record RaftLogEntry<T extends RsmRequest>(int term, T cmd, long timestamp) {

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putLong(timestamp);
        cmd.serialize(buffer);
    }

    @Override
    public String toString() {
        return "RLE{" +
                "term=" + term +
                " cmd=" + cmd +
                '}';
    }

    public static <T extends RsmRequest> RaftLogEntry<T> create(ByteBuffer buffer,
                                                                RsmRequestFactory<T> factory) {
        final int term = buffer.getInt();
        final long timestamp = buffer.getLong();
        final T cmd = factory.createRequest(buffer);
        return new RaftLogEntry<>(term, cmd, timestamp);
    }

    public static <T extends RsmRequest> RaftLogEntry<T> create(DataInputStream dis,
                                                                RsmRequestFactory<T> factory) throws IOException {
        final int term = dis.readInt();
        final long timestamp = dis.readLong();
        final T cmd = factory.createRequest(dis);
        return new RaftLogEntry<>(term, cmd, timestamp);
    }
}
