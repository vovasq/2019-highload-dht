package ru.mail.polis.dao.adapter;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.util.Iterator;

public interface LsmAdapter {

    Iterator<Record> iterator(@NotNull byte[] from) throws IOException;

    void put(@NotNull byte[] key, @NotNull byte[] value) throws IOException;

    void remove(@NotNull byte[] key) throws IOException;

    void close() throws IOException;
}
