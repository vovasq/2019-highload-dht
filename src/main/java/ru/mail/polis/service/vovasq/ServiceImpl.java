package ru.mail.polis.service.vovasq;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static ru.mail.polis.util.Util.fromByteBufferToByteArray;

public class ServiceImpl extends HttpServer implements Service {


    private final DAO dao;


    public ServiceImpl(int port, DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/status")
    public Response status(
            @Param("id") String id,
            Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(
            @Param("id") String id,
            Request request) {
        if (id == null || id.isEmpty()) return new Response(Response.BAD_REQUEST, Response.EMPTY);

        ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        switch (request.getMethod()) {
            case Request.METHOD_GET:
//                System.out.println("get with id = " + id);
                try {
                    ByteBuffer value = dao.get(key);
                    return new Response(Response.OK, fromByteBufferToByteArray(value));
                } catch (NoSuchElementException e) {
                    return new Response(Response.NOT_FOUND, "No such a key".getBytes(Charsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                    return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                }
            case Request.METHOD_PUT:
                System.out.println("put with id = " + id);
                System.out.println("delete with id = " + id);
                try {
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);
                } catch (IOException e) {
                    return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                }
            case Request.METHOD_DELETE:
                try {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                } catch (IOException e) {
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                }
            default:
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }


    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    private static HttpServerConfig getConfig(int port) {
        if (port < 1024 || port > 65535) throw new IllegalArgumentException("Illegal port");
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }
}
