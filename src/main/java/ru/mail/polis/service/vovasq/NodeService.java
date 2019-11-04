package ru.mail.polis.service.vovasq;

import com.google.common.base.Charsets;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Session;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
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
import static ru.mail.polis.util.ByteUtil.fromByteBufferToByteArray;

public class NodeService extends HttpServer implements Service {

    private final DAO dao;
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final String HTTP_INITIATOR_HEADER = "X-INITIATOR:";
    private final String HTTP_TIMESTAMP_HEADER = "X-TIMESTAMP:";
    private final Topology<String> topology;
    private final Map<String, HttpClient> neighbours;

    public NodeService(final int port, final DAO dao,
                       final int minNumOfWorkers,
                       final int maxNumOfWorkers,
                       final Topology<String> topology) throws IOException {
        super(getConfig(port, minNumOfWorkers, maxNumOfWorkers));
        this.dao = dao;
        this.topology = topology;
        neighbours = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            neighbours.put(node, new HttpClient(new ConnectionString(node)));
        }
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
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StorageSession(socket, this);
    }

    private Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    private void updateRequestHeader(@NotNull Request request) {
        request.addHeader(HTTP_INITIATOR_HEADER + " YES");
        request.addHeader(HTTP_TIMESTAMP_HEADER + " " + System.currentTimeMillis());
    }

    private boolean isExternalRequest(Request request) {
        return request.getHeader(HTTP_INITIATOR_HEADER) == null;
    }

    private void entity(
            @NotNull final Request request,
            @NotNull final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final String primaryNode = topology.primaryFor(key);
        if (isExternalRequest(request)) {
            try {
                // TODO: async adding with new method
                ReplicasParams params = ReplicasParams.of(request.getParameter("replicas="), neighbours.size());
                log.info("get replicas params: {}/{}", params.getAck(), params.getFrom());
                updateRequestHeader(request);
                final ReplicasManager replicasManager =
                        new ReplicasManager(params, topology, key);
                final List<String> nodesToCall = replicasManager.getNodesToCall();
                replicateAndRespond(nodesToCall, key, session, request, params);
            } catch (IllegalArgumentException e) {
                session.sendResponse(new Response(Response.BAD_REQUEST,
                        "Wrong format for replicas params".getBytes(Charset.defaultCharset())));
            }
            return;
        }
        if (!topology.isMe(primaryNode)) {
            asyncExecute(() -> {
                askAnotherNodeAndHandle(request, session, primaryNode);
            });
            return;
        }
        asyncExecute(() -> {
            handleMyRequest(request, session, key);
        });
    }

    private void replicateAndRespond(@NotNull final List<String> nodes,
                                     @NotNull final ByteBuffer key,
                                     @NotNull final HttpSession session,
                                     @NotNull final Request request,
                                     @NotNull final ReplicasParams params) {
        final List<Response> responses = new ArrayList<>();
        for (String node : nodes) {
            if (isEnoughPositiveResponses(responses, params.getAck())) {
                try {
                    session.sendResponse(new Response(Response.OK,Response.EMPTY));
                } catch (IOException e) {
                    log.error("error while sending response to client, caused by: {}", e);
                }
            }
            if (topology.isMe(node)) {
                responses.add(getMyResponse(request, key));
            } else {
                try {
                    responses.add(proxy(node, request));
                } catch (InterruptedException | IOException | HttpException | PoolException ignored) {
                    responses.add(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            }
        }

    }

    private boolean isEnoughPositiveResponses(final @NotNull List<Response> responses, final int ack) {
        return responses.size() != 0 && ack > 0;
    }

    private void entities(@NotNull final Request request,
                          @NotNull final HttpSession session) throws IOException {
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

    private Response getMyResponse(@NotNull final Request request, @NotNull final ByteBuffer key) {
        Response response;
        try {
            response = determineRequest(request, key);
        } catch (IOException e) {
            response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return response;
    }

    private void handleMyRequest(@NotNull final Request request,
                                 @NotNull final HttpSession session,
                                 @NotNull final ByteBuffer key) {
        try {
            session.sendResponse(getMyResponse(request, key));
        } catch (IOException exc) {
            log.error("Error caused by: ", exc);
        }
    }

    private Response determineRequest(@NotNull final Request request, @NotNull final ByteBuffer key) throws IOException {
        Response response;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                response = get(key);
                break;
            case Request.METHOD_PUT:
                response = upsert(key, request.getBody());
                break;
            case Request.METHOD_DELETE:
                response = remove(key);
                break;
            default:
                response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                break;
        }
        return response;
    }

    private void askAnotherNodeAndHandle(@NotNull final Request request,
                                         @NotNull final HttpSession session,
                                         @NotNull final String primaryNode) {
        try {
            session.sendResponse(proxy(primaryNode, request));
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            Response response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            try {
                session.sendResponse(response);
            } catch (IOException exc) {
                log.error("Error caused by: ", exc);
            }
        }
    }

    private Response proxy(@NotNull final String node, @NotNull final Request request) throws InterruptedException, IOException, HttpException, PoolException {
        try {
            return neighbours.get(node).invoke(request, 100);
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            throw e;
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

    private Response remove(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response upsert(final ByteBuffer key, final byte[] body) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(body));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static HttpServerConfig getConfig(final int port, final int minNumOfWorkers, final int maxNumOfWorkers) {
        if (port < 1024 || port > 65535) throw new IllegalArgumentException("Illegal port");
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        config.minWorkers = minNumOfWorkers;
        config.maxWorkers = maxNumOfWorkers;
        return config;
    }
}
