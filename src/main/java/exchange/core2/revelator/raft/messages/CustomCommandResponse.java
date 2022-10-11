package exchange.core2.revelator.raft.messages;

import exchange.core2.revelator.raft.RsmResponseFactory;

import java.nio.ByteBuffer;

public record CustomCommandResponse<S extends RsmResponse>(S rsmResponse,
                                                           int leaderNodeId,
                                                           boolean success) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RESPONSE_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(leaderNodeId);
        buffer.putInt(success ? 1 : 0);
        rsmResponse.serialize(buffer);
    }

    public static <S extends RsmResponse> CustomCommandResponse<S> create(ByteBuffer buffer, RsmResponseFactory<S> factory) {

        final int leaderNodeId = buffer.getInt();
        final boolean success = buffer.getInt() == 1;
        final S rsmResponse = factory.createResponse(buffer);

        return new CustomCommandResponse<>(rsmResponse, leaderNodeId, success);
    }

}
