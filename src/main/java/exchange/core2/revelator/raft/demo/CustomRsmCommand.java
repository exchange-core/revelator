package exchange.core2.revelator.raft.demo;

import exchange.core2.revelator.raft.messages.RsmRequest;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public record CustomRsmCommand(long data) implements RsmRequest {

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    @Override
    public String toString() {
        return "CRC{" +
                "data=" + data +
                '}';
    }

    public static CustomRsmCommand create(ByteBuffer buffer) {
        return new CustomRsmCommand(buffer.getLong());
    }

    public static CustomRsmCommand create(DataInputStream dis) throws IOException {
        return new CustomRsmCommand(dis.readLong());
    }

}
