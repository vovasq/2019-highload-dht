package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.adapter.LsmAdapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class SimpleDaoImpl implements DAO {

    private final LsmAdapter dbAdapter;

    public SimpleDaoImpl(@NotNull LsmAdapter dbAdapter) {
        this.dbAdapter = dbAdapter;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return dbAdapter.iterator(from.array());
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        dbAdapter.put(key.array(), value.array());
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        dbAdapter.remove(key.array());
    }

    @Override
    public void close() throws IOException {
        dbAdapter.close();
    }
}
