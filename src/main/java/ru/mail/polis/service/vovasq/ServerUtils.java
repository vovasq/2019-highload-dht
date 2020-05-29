package ru.mail.polis.service.vovasq;


import one.nio.http.HttpClient;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Timestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class ServerUtils {

    private ServerUtils() {
    }

    /**
     * Method to get for HttpServerConfig.
     *
     * @param port - int port
     * @return - HttpServerConfig
     */
    static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    static void entities(@NotNull final Request request,
                         @NotNull final HttpSession session,
                         final DAO dao) throws IOException {
        final String startKey = "start=";
        final String startId = request.getParameter(startKey);

        if (isNotNullOrEmpty(startId)) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        final String wrong_method = "Wrong method";
        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, wrong_method);
            return;
        }

        final String endKey = "end=";
        String end = request.getParameter(endKey);
        if (end != null && end.isEmpty()) end = null;

        try {
            final ByteBuffer wrap = ByteBuffer.wrap(startId.getBytes(Charset.defaultCharset()));
            final Iterator<Record> records = dao.range(wrap,
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charset.defaultCharset())));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    static boolean isNotNullOrEmpty(final String startId) {
        return startId == null || startId.isEmpty();
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
        if (nodes.length == 1) {
            return new Response(Response.OK, mergeResponse.getPresentAsBytes());
        }
        return new Response(Response.OK, mergeResponse.getPresentAsBytes());
    }

    static void execute(@NotNull final HttpSession session, @NotNull final Action action, final Logger log) {
        try {
            session.sendResponse(action.act());
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, e.getMessage());
            } catch (IOException ex) {
                log.error(ex.getMessage());
            }
        }
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

    static Map<String, HttpClient> getPool(final Topology<String> topology) {
        final Map<String, HttpClient> pool = new HashMap<>();
        for (final String node : topology.getAll()) {
            if (topology.isMe(node)) {
                continue;
            }

            assert !pool.containsKey(node);
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
        return pool;
    }
}
