package exchange.core2.revelator.raft.messages;

import java.nio.ByteBuffer;

public interface SerializableMessage {

    void serialize(ByteBuffer buffer);

}
