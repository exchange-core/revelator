package exchange.core2.revelator.raft;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class RaftClient {

    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);


    public static void main(String[] args) throws IOException, InterruptedException {
        final RaftClient raftClient = new RaftClient();

        Random random = new Random(1L);
        while (true) {
            raftClient.sendEcho(random.nextLong());
            Thread.sleep(2000);
        }
    }

    private RpcClient rpcClient;

    public RaftClient() {

        // localhost:3778, localhost:3779, localhost:3780
        final Map<Integer, String> remoteNodes = Map.of(
                0, "localhost:3778",
                1, "localhost:3779",
                2, "localhost:3780");

        this.rpcClient = new RpcClient(remoteNodes);
    }

    public void sendEcho(long data) {
        try {
            log.info("send >>> data={}", data);
            final int hash = rpcClient.callRpcSync(data, 500);
            log.info("recv <<< hash={}", hash);
        } catch (Exception ex) {
            log.warn("Exception: ", ex);
        }
    }

}