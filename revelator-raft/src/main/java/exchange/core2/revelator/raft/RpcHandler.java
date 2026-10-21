package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.*;

import java.net.InetAddress;

public interface RpcHandler<T extends RsmRequest, S extends RsmResponse> {

    RpcResponse handleNodeRequest(int nodeId, RpcRequest request);

    void handleNodeResponse(int nodeId, RpcResponse response, long correlationId);

    CustomCommandResponse<S> handleClientRequest(InetAddress address,
                                                 int port,
                                                 long correlationId,
                                                 long timeReceived,
                                                 CustomCommandRequest<T> request);

}
