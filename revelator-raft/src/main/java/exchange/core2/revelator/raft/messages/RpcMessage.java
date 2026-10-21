package exchange.core2.revelator.raft.messages;

import java.nio.ByteBuffer;

public interface RpcMessage {

    int getMessageType();

    void serialize(ByteBuffer buffer);

    int REQUEST_APPEND_ENTRIES = 1;
    int RESPONSE_APPEND_ENTRIES = -1;
    int REQUEST_VOTE = 2;
    int RESPONSE_VOTE = -2;

    int REQUEST_CUSTOM = 10;
    int RESPONSE_CUSTOM = -10;

}
