package ru.mail.polis.util;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Util {
    public static byte[] fromByteBufferToByteArray(@NotNull ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] array = new byte[duplicate.remaining()];
        duplicate.get(array);
        return array;
    }

}
