package ru.mail.polis.util;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Util {

    private Util() {
    }

    /*
     *  function convert from ByteBuffer to byte array
     */

    public static byte[] fromByteBufferToByteArray(@NotNull ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] array = new byte[duplicate.remaining()];
        duplicate.get(array);
        return array;
    }
}
