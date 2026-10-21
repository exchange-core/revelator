package exchange.core2.revelator.raft.messages;

import exchange.core2.revelator.raft.RsmRequestFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

// TODO support batching !!

public record CustomCommandRequest<T extends RsmRequest>(T rsmRequest) implements RpcRequest {

    @Override
    public int getMessageType() {
        return REQUEST_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        rsmRequest.serialize(buffer);
    }

    public static <T extends RsmRequest> CustomCommandRequest<T> create(
            final ByteBuffer buffer,
            final RsmRequestFactory<T> factory) {

        return new CustomCommandRequest<>(factory.createRequest(buffer));
    }


    public static <T extends RsmRequest> CustomCommandRequest<T> create(
            final DataInputStream dis,
            final RsmRequestFactory<T> factory) throws IOException {

        return new CustomCommandRequest<>(factory.createRequest(dis));
    }
}
