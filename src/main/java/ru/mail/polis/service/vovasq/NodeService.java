package ru.mail.polis.service.vovasq;

import com.google.common.base.Charsets;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.RocksDaoImpl;
import ru.mail.polis.dao.Timestamp;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static ru.mail.polis.service.vovasq.ServerUtils.getConfig;
import static ru.mail.polis.util.Util.fromByteBufferToByteArray;

public class NodeService extends HttpServer implements Service {

    private static final String ENTITY_ID = "/v0/entity?id=";
    private static final String PROXY_HEADER = "PROXY_HEADER";
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final RocksDaoImpl dao;
    private final Topology<String> topology;
    private final Map<String, HttpClient> neighbours;
    private final Replica defaultReplica;
    private final int size;

    public NodeService(final int port, final DAO dao,
                       final int minNumOfWorkers,
                       final int maxNumOfWorkers,
                       final Topology<String> topology) throws IOException {
        super(getConfig(port, minNumOfWorkers, maxNumOfWorkers));
        this.dao = (RocksDaoImpl) dao;
        this.topology = topology;
        this.neighbours = new HashMap<>();
        this.size = topology.getAll().size();
        for (final String node : topology.getAll()) {
            if (topology.isMe(node)) {
                continue;
            }
            neighbours.put(node, new HttpClient(new ConnectionString(node)));
        }
        this.defaultReplica = new Replica(size / 2 + 1, size);
    }


    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "/v0/status":
                session.sendResponse(status());
                break;
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                break;
        }
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    private void entity(
            @NotNull final Request request,
            @NotNull final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        boolean proxied = true;
        if (request.getHeader(PROXY_HEADER) == null) {
            proxied = false;
        }

        final String replicaParam = request.getParameter("replicas");
        final Replica newReplica = Replica.of(replicaParam, defaultReplica);
        if (newReplica.ack < 1 || newReplica.from < newReplica.ack || newReplica.from > size) {
            session.sendError(Response.BAD_REQUEST, "Wrong replicas params from = " + newReplica.from + ", ack = " + newReplica.ack);
        }


        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        if (proxied || size > 1) {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        session.sendResponse(get(proxied, newReplica, id));
                        break;

                    case Request.METHOD_PUT:
                        session.sendResponse(upsert(proxied, request.getBody(), newReplica.ack, id));
                        break;

                    case Request.METHOD_DELETE:
                        session.sendResponse(remove(proxied, newReplica.ack, id));
                        break;

                    default:
                        session.sendError(Response.METHOD_NOT_ALLOWED, "Proxied wrong method");
                        break;
                }
            } catch (IOException e) {
                session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
            }
        } else {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(key));
                    break;

                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(key, request.getBody()));
                    break;

                case Request.METHOD_DELETE:
                    executeAsync(session, () -> remove(key));
                    break;

                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Non-proxied wrong method");
                    break;
            }
        }
    }

    private void entities(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "NO Start");
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong Method");
            return;
        }
        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }
        try {
            final Iterator<Record> records = dao.range(ByteBuffer.wrap(start.getBytes(UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }


    }

    private Response get(final ByteBuffer key) throws IOException {
        Response response;
        try {
            final ByteBuffer value = dao.get(key);
            response = new Response(Response.OK, fromByteBufferToByteArray(value));
        } catch (NoSuchElementException e) {
            response = new Response(Response.NOT_FOUND, "No such a key".getBytes(Charsets.UTF_8));
        }
        return response;
    }

    private Response get(final boolean proxied, final Replica rf, final String id) throws IOException {
        final String[] nodes = ServerUtils.getNodes(proxied, rf, id, topology);
        int acks = 0;
        final List<Timestamp> responses = new ArrayList<>();
        for (final String node : nodes) {
            try {
                Response response;
                if (topology.isMe(node)) {
                    final ByteBuffer byteBuffer = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
                    final Timestamp timestamp = dao.getWithTimestamp(byteBuffer);
                    if (timestamp.isAbsent()) {
                        response = new Response(Response.NOT_FOUND, Response.EMPTY);
                    } else {
                        response = new Response(Response.OK, timestamp.timestampToBytes());
                    }
                } else {
                    response = neighbours.get(node).get(ENTITY_ID + id, PROXY_HEADER);
                }
                if (response.getStatus() == 404 && response.getBody().length == 0) {
                    responses.add(Timestamp.getAbsentTimestamp());
                } else if (response.getStatus() != 500) {
                    responses.add(Timestamp.getTimestampFromBytes(response.getBody()));
                }
                acks++;
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                log.info("get method", e);
            }
        }
        return ServerUtils.acksMoreThanRfAck(proxied, rf, nodes, acks, responses);
    }

    private Response upsert(final ByteBuffer key, final byte[] body) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(body));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response upsert(final boolean proxied, final byte[] value, final int ack, final String id) {
        final ByteBuffer wrap = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
        if (proxied) {
            try {
                dao.upsertWithTimestamp(wrap, ByteBuffer.wrap(value));
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }

        final String[] nodes = ServerUtils.replicas(wrap, defaultReplica.from, topology);

        int acks = 0;
        for (final String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.upsertWithTimestamp(wrap, ByteBuffer.wrap(value));
                    acks++;
                } else {
                    final Response response = neighbours.get(node).put(ENTITY_ID + id, value, PROXY_HEADER);
                    if (response.getStatus() == 201) {
                        acks++;
                    }
                }
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                log.info("upsert method", e);
            }
        }
        if (acks >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response remove(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response remove(final boolean proxied, final int ack, final String id) {
        final ByteBuffer wrap = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
        if (proxied) {
            try {
                dao.removeWithTimestamp(wrap);
                return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }

        final String[] nodes = ServerUtils.replicas(wrap, defaultReplica.from, topology);

        int acks = 0;
        for (final String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.removeWithTimestamp(wrap);
                    acks++;
                } else {
                    final Response response = neighbours.get(node).delete(ENTITY_ID + id, PROXY_HEADER);
                    if (response.getStatus() == 202) {
                        acks++;
                    }
                }
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                log.info("delete method", e);
            }
            if (acks >= ack) {
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    @FunctionalInterface
    private interface Action {
        Response act() throws IOException;
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        asyncExecute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    log.error(ex.getMessage());
                }
            }
        });
    }

}
