package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.RsmRequest;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface RsmRequestFactory<T extends RsmRequest> {

    T createRequest(ByteBuffer buffer);

    T createRequest(DataInputStream dis) throws IOException;

}
