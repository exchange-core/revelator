package exchange.core2.revelator.raft.demo;

import exchange.core2.revelator.raft.RaftNode;
import exchange.core2.revelator.raft.repository.IRaftLogRepository;
import exchange.core2.revelator.raft.repository.RaftMemLogRepository;

public class CustomNode {

    public static void main(String[] args) {

        final int thisNodeId = Integer.parseInt(args[0]);

        final CustomRsm customRsm = new CustomRsm();

        //final RaftDiskLogRepository<CustomRsmCommand> repository = new RaftDiskLogRepository<>(customRsm, thisNodeId);
        final IRaftLogRepository<CustomRsmCommand> repository = new RaftMemLogRepository<>();

        new RaftNode<>(thisNodeId, repository, customRsm, customRsm, customRsm);
    }

}
