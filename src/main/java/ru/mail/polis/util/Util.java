package ru.mail.polis.util;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Util {

    private Util() {
    }

    /**
     * function converts from ByteBuffer to byte array.
     * @param buffer argument to convert
     * @return byte array
     */
    public static byte[] fromByteBufferToByteArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] array = new byte[duplicate.remaining()];
        duplicate.get(array);
        return array;
    }
}
