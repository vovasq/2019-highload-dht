package ru.mail.polis.service.vovasq;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Set;

public interface Topology<T> {

    boolean isMe(@NotNull T node);

    @NotNull
    T primaryFor(@NotNull ByteBuffer key);

    @NotNull
    Set<T> all();

}
