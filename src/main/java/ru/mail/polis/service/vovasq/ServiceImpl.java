package ru.mail.polis.service.vovasq;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.util.Util.fromByteBufferToByteArray;

public class ServiceImpl extends HttpServer implements Service {


    private final DAO dao;
    private final static ReadWriteLock LOCK=new ReentrantReadWriteLock();
    private final static Lock readLock = LOCK.readLock();
    private final static Lock writeLock = LOCK.writeLock();


    public ServiceImpl(final int port, final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/status")
    public Response status(
            @Param("id") String id,
            Request request) {
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
        Response response;
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            writeLock.lock();
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    response = getFromDao(key);
                    break;
                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    response = new Response(Response.CREATED, Response.EMPTY);
                    break;
                case Request.METHOD_DELETE:
                    dao.remove(key);
                    response = new Response(Response.ACCEPTED, Response.EMPTY);
                    break;
                default:
                    response =  new Response(Response.BAD_REQUEST, Response.EMPTY);
                    break;
            }
        } catch (IOException e) {
            response =  new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } finally {
            writeLock.unlock();
        }
        return response;
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
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

    private Response getFromDao(final ByteBuffer key) throws IOException{
        Response response;
        try {
            final ByteBuffer value = dao.get(key);
            response = new Response(Response.OK, fromByteBufferToByteArray(value));
        } catch (NoSuchElementException e) {
            response = new Response(Response.NOT_FOUND, "No such a key".getBytes(Charsets.UTF_8));
        }
        return response;
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
