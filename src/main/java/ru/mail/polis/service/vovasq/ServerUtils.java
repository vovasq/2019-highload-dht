package ru.mail.polis.service.vovasq;


import one.nio.http.HttpServerConfig;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Timestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

class ServerUtils {

    private ServerUtils() {
    }

    /**
     * Method to get for HttpServerConfig.
     *
     * @param port            - int port
     * @param minNumOfWorkers - min number of worker threads
     * @param maxNumOfWorkers - min number of worker threads
     * @return - HttpServerConfig
     */
    static HttpServerConfig getConfig(final int port, final int minNumOfWorkers, final int maxNumOfWorkers) {
        if (port < 1024 || port > 65535) throw new IllegalArgumentException("Illegal port");
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        config.minWorkers = minNumOfWorkers;
        config.maxWorkers = maxNumOfWorkers;
        return config;
    }

    static String[] replicas(final ByteBuffer id, final int count, final Topology<String> topology) {
        final String[] result = new String[count];
        final List<String> nodes = topology.getAll();
        int ind = (id.hashCode() & Integer.MAX_VALUE) % nodes.size();
        for (int j = 0; j < count; j++) {
            result[j] = nodes.get(ind);
            ind = (ind + 1) % nodes.size();
        }

        return result;
    }

    static Response acksMoreThanRfAck(final boolean proxied,
                                      final Replica replica,
                                      final String[] nodes,
                                      final int acks,
                                      final List<Timestamp> responses) throws IOException {
        if (acks >= replica.ack || proxied) {
            final Timestamp mergeResponse = Timestamp.merge(responses);
            if (mergeResponse.isPresent()) {
                return getResponse(proxied, nodes, mergeResponse);
            }
            if (mergeResponse.isRemoved()) {
                return new Response(Response.NOT_FOUND, mergeResponse.timestampToBytes());
            }
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    @NotNull
    private static Response getResponse(final boolean proxied,
                                        final String[] nodes,
                                        final Timestamp mergeResponse) throws IOException {
        if (proxied) {
            return new Response(Response.OK, mergeResponse.timestampToBytes());
        }
        return new Response(Response.OK, mergeResponse.getPresentAsBytes());
    }

    static String[] getNodes(final boolean proxied, final Replica replica, final String id, final Topology<String> topology) {
        final String[] nodes;
        if (proxied) {
            nodes = new String[]{getMe(topology)};
        } else {
            nodes = ServerUtils.replicas(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), replica.from, topology);
        }
        return nodes;
    }

    static String getMe(final Topology<String> topology) {
        for (final String node : topology.getAll()) {
            if (topology.isMe(node)) {
                return node;
            }
        }
        return null;
    }
}
