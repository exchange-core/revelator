package exchange.core2.revelator.raft.demo;

import exchange.core2.revelator.raft.messages.RsmResponse;

import java.nio.ByteBuffer;

public record CustomRsmResponse(int hash) implements RsmResponse {

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(hash);
    }

    @Override
    public String toString() {
        return "CRR{" +
                "hash=" + hash +
                '}';
    }

    public static CustomRsmResponse create(ByteBuffer buffer) {
        return new CustomRsmResponse(buffer.getInt());
    }
}
