package ru.mail.polis.service.vovasq;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static ru.mail.polis.util.ByteUtil.fromByteBufferToByteArray;

public class StorageSession extends HttpSession {

    private static final byte[] CRLF = "\r\n".getBytes(UTF_8);
    private static final byte[] LF = "\n".getBytes(UTF_8);
    private static final byte[] EMPTY_CHUNK = "0\r\n\r\n".getBytes(UTF_8);

    private Iterator<Record> records;

    public StorageSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    void stream(@NotNull final Iterator<Record> records) throws IOException {
        this.records = records;
        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);
        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private byte[] buildChunk() {
        final Record record = records.next();
        final byte[] key = fromByteBufferToByteArray(record.getKey());
        final byte[] value = fromByteBufferToByteArray(record.getValue());
//            <key>'\n'<value>
        final int payloadLength = key.length + 1 + value.length;
        final String size = Integer.toHexString(payloadLength);
        // <size>\r\n<payload>\r\n
        final int chunkLength = size.length() + 2 + payloadLength + 2;
        final byte[] chunk = new byte[chunkLength];
        final ByteBuffer buffer = ByteBuffer.wrap(chunk);
        buffer.put(size.getBytes(UTF_8));
        buffer.put(CRLF);
        buffer.put(key);
        buffer.put(LF);
        buffer.put(value);
        buffer.put(CRLF);
        return chunk;
    }

    private void next() throws IOException {

        while (records.hasNext() && queueHead == null) {
            final byte[] chunk = buildChunk();
            write(chunk, 0, chunk.length);
        }
        if (!records.hasNext()) {
            write(EMPTY_CHUNK, 0, EMPTY_CHUNK.length);

            server.incRequestsProcessed();

            if ((handling = pipeline.pollFirst()) != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    try {
                        server.handleRequest(handling, this);
                    } catch (IOException e) {
                        log.error("Can't proccess next request: " + handling, e);
                    }
                }
            }
        }
    }
}
