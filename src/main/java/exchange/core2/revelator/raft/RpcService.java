package exchange.core2.revelator.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class RpcService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final AtomicLong correlationIdCounter = new AtomicLong(0L);
    private final Map<Long, CompletableFuture<RpcResponse>> futureMap = new ConcurrentHashMap<>();
    private final Map<Integer, RemoteUdpSocket> socketMap;
    private final int serverPort;
    private final int serverNodeId;
    private final BiFunction<Integer, RpcRequest, RpcResponse> handler;

    private volatile boolean active = true;

    public RpcService(Map<Integer, String> remoteNodes,
                      BiFunction<Integer, RpcRequest, RpcResponse> handler,
                      int serverNodeId) {

        final Map<Integer, RemoteUdpSocket> socketMap = new HashMap<>();
        remoteNodes.forEach((id, address) -> {

            try {
                final String[] split = address.split(":");

                final DatagramSocket socket = new DatagramSocket();
                final InetAddress host = InetAddress.getByName(split[0]);
                final int port = Integer.parseInt(split[1]);

                RemoteUdpSocket remoteUdpSocket = new RemoteUdpSocket(socket, host, port);

                socketMap.put(id, remoteUdpSocket);

            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });


        this.socketMap = socketMap;
        this.handler = handler;
        this.serverPort = socketMap.get(serverNodeId).port;
        this.serverNodeId = serverNodeId;

        Thread t = new Thread(this::run);
        t.setDaemon(true);
        t.setName("ListenerUDP");
        t.start();

    }


    public void run() {

        try (final DatagramSocket serverSocket = new DatagramSocket(serverPort)) {

            logger.info("Listening on UDP {}:{}", InetAddress.getLocalHost().getHostAddress(), serverPort);

            final byte[] receiveData = new byte[256];

            final DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            while (active) {

                serverSocket.receive(receivePacket);
                // String sentence = new String(receivePacket.getData(), 0, receivePacket.getLength());
//                logger.debug("RECEIVED: " + sentence);


                final ByteBuffer bb = ByteBuffer.wrap(receivePacket.getData(), 0, receivePacket.getLength());

                final int nodeId = bb.getInt();
                final int messageType = bb.getInt();
                final long correlationId = bb.getLong();

                if (messageType < 0) {
                    processResponse(receivePacket, bb, messageType, correlationId);

                } else {
                    processRequest(receivePacket, bb, nodeId, messageType, correlationId);
                }
            }

            logger.info("UDP server shutdown");

        } catch (final IOException ex) {
            logger.error("Error in service thread", ex);
            throw new RuntimeException(ex);
        }

    }

    private void processRequest(DatagramPacket receivePacket, ByteBuffer bb, int nodeId, int messageType, long correlationId) {

        if (messageType == RpcRequest.REQUEST_APPEND_ENTRIES) {

            final CmdRaftAppendEntries request = CmdRaftAppendEntries.create(bb);
            final RpcResponse response = handler.apply(nodeId, request);
            sendResponse(nodeId, correlationId, response);

        } else if (messageType == RpcRequest.REQUEST_VOTE) {

            final CmdRaftVoteRequest request = CmdRaftVoteRequest.create(bb);
            final RpcResponse response = handler.apply(nodeId, request);
            sendResponse(nodeId, correlationId, response);

        } else {
            logger.warn("Unsupported response type={} from {} correlationId={}",
                    messageType, receivePacket.getAddress().getHostAddress(), correlationId);
        }
    }

    private void processResponse(DatagramPacket receivePacket, ByteBuffer bb, int messageType, long correlationId) {
        final CompletableFuture<RpcResponse> future = futureMap.remove(correlationId);

        if (future != null) {

            if (messageType == RpcResponse.RESPONSE_APPEND_ENTRIES) {

                future.complete(CmdRaftAppendEntriesResponse.create(bb));

            } else if (messageType == RpcResponse.RESPONSE_VOTE) {

                future.complete(CmdRaftVoteResponse.create(bb));

            } else {
                logger.warn("Unsupported response type={} from {} correlationId={}",
                        messageType, receivePacket.getAddress().getHostAddress(), correlationId);
            }


        } else {
            logger.warn("Unexpected (or duplicate) response from {} type={} correlationId={}",
                    receivePacket.getAddress().getHostAddress(), messageType, correlationId);
        }
    }

    private void sendResponse(int callerNodeId, long correlationId, RpcResponse response) {
        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(serverNodeId);
        bb.putInt(response.getMessageType());
        bb.putLong(correlationId);
        response.serialize(bb);

        send(callerNodeId, array, bb.position());
    }


    public CompletableFuture<RpcResponse> callRpcSync(RpcRequest request, int nodeId) {

        final long correlationId = correlationIdCounter.incrementAndGet();

        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        futureMap.put(correlationId, future);

        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(nodeId);
        bb.putInt(request.getMessageType());
        bb.putLong(correlationId);

        request.serialize(bb);

        send(nodeId, array, bb.position());

        return future;
    }


    private void send(int nodeId, byte[] data, int length) {

        final RemoteUdpSocket remoteUdpSocket = socketMap.get(nodeId);
        final DatagramPacket packet = new DatagramPacket(data, length, remoteUdpSocket.address, remoteUdpSocket.port);

        try {
            remoteUdpSocket.socket.send(packet);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    public void close() throws Exception {

        active = false;
    }


    public static final class RemoteUdpSocket {

        private final DatagramSocket socket;
        private final InetAddress address;
        private final int port;

        public RemoteUdpSocket(DatagramSocket socket, InetAddress address, int port) {
            this.socket = socket;
            this.address = address;
            this.port = port;
        }
    }


}
