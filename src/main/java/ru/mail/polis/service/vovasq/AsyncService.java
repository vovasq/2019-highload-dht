package ru.mail.polis.service.vovasq;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static ru.mail.polis.util.Util.fromByteBufferToByteArray;


public class AsyncService extends HttpServer implements Service {


    private final DAO dao;
    private static final Log log = LogFactory.getLog(AsyncService.class);


    public AsyncService(final int port, final DAO dao,
                        final int minNumOfWorkers,
                        final int maxNumOfWorkers) throws IOException {
        super(getConfig(port, minNumOfWorkers, maxNumOfWorkers));
        this.dao = dao;
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

    private void entity(
            @NotNull final Request request,
            @NotNull final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        asyncExecute(() -> {
            Response response;
            try {
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
            } catch (IOException e) {
                response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
            try {
                session.sendResponse(response);
            } catch (IOException exc) {
                log.error("Error caused by: {}", exc);
            }
        });
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

