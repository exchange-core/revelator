package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.RsmResponse;

import java.nio.ByteBuffer;

public interface RsmResponseFactory<S extends RsmResponse> {

    S createResponse(ByteBuffer buffer);

    S emptyResponse();
}
