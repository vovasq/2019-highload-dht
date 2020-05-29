/*
 * Copyright 2019 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Contains utility methods for unit tests.
 *
 * @author Vadim Tsesko
 */
public abstract class TestBase {
    protected static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 1024;

    protected static int randomPort() {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 0), 1);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Can't discover a free port", e);
        }
    }

    @NotNull
    protected static String randomId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    @NotNull
    private static byte[] randomBytes(final int length) {
        assert length > 0;
        final byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    @NotNull
    protected static ByteBuffer randomBuffer(final int length) {
        return ByteBuffer.wrap(randomBytes(length));
    }

    @NotNull
    protected static byte[] randomValue() {
        return randomBytes(VALUE_LENGTH);
    }

    @NotNull
    public static ByteBuffer randomKeyBuffer() {
        return randomBuffer(KEY_LENGTH);
    }

    @NotNull
    protected static ByteBuffer randomValueBuffer() {
        return randomBuffer(VALUE_LENGTH);
    }

    @NotNull
    protected static ByteBuffer join(
            @NotNull final ByteBuffer left,
            @NotNull final ByteBuffer right) {
        final ByteBuffer result = ByteBuffer.allocate(left.remaining() + right.remaining());
        result.put(left.duplicate());
        result.put(right.duplicate());
        result.rewind();
        return result;
    }

    @NotNull
    protected static String endpoint(final int port) {
        return "http://localhost:" + port;
    }
}
