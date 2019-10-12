package ru.mail.polis.service.vovasq;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static ru.mail.polis.util.Util.fromByteBufferToByteArray;

public class ServiceImpl extends HttpServer implements Service {


    private final DAO dao;

    public ServiceImpl(final int port, final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    /*
    * end point for lifecheck
    * */
    @Path("/v0/status")
    public Response status(
            @Param("id") final String id,
            final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    /*
     * simple REST API to DAO according to README.md
     * */
    @Path("/v0/entity")
    public Response entity(
            @Param("id") final String id,
            final Request request) {
        if (id == null || id.isEmpty()) return new Response(Response.BAD_REQUEST, Response.EMPTY);
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(key);
                case Request.METHOD_PUT:
                    return upsert(key, request.getBody());
                case Request.METHOD_DELETE:
                    return remove(key);
                default:
                    return new Response(Response.BAD_REQUEST, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
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

    private static HttpServerConfig getConfig(final int port) {
        if (port < 1024 || port > 65535) throw new IllegalArgumentException("Illegal port");
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }
}
