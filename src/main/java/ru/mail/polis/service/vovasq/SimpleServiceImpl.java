package ru.mail.polis.service.vovasq;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.util.logging.Logger;

public class SimpleServiceImpl extends HttpServer implements Service {


    private final DAO dao;


    public SimpleServiceImpl(int port, DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/entity")
    public Response entity(
            @Param("id") String id,
            Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return new Response(Response.OK, "LOL\n".getBytes());
            case Request.METHOD_PUT:
//                return new Response(Response.OK,  "LOL".getBytes());
            default:
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
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
