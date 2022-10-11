package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.RsmRequest;
import exchange.core2.revelator.raft.messages.RsmResponse;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

public interface ReplicatedStateMachine<T extends RsmRequest, S extends RsmResponse> extends WriteBytesMarshallable {

    /**
     * Changes state of Replicated State Machine
     *
     * @param command command
     * @return result
     */
    S applyCommand(T command);

    // TODO query

    /**
     * Execute a query that does not change the state
     *
     * @param query query
     * @return query result
     */
    S applyQuery(T query);

}
