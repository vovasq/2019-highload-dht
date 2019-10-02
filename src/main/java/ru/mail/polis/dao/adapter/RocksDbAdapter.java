package ru.mail.polis.dao.adapter;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

public class RocksDbAdapter implements LsmAdapter {

    private RocksDB db;


    public RocksDbAdapter(@NotNull File data) throws RocksDBException {
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try {
            final Options options = new Options().setCreateIfMissing(true);
            // a factory method that returns a RocksDB instance
            db = RocksDB.open(options, data.getPath());
        } catch (RocksDBException e) {
            System.out.println(e);
        }
    }


    @Override
    public Iterator<Record> iterator(@NotNull byte[] from) throws IOException {

        RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seek(from);
        if (Arrays.equals(rocksIterator.key(), from))
            System.out.println("yes seek");
        return new RocksDbToRecordIterator(rocksIterator);
    }

    @Override
    public void put(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    private static class RocksDbToRecordIterator implements Iterator<Record> {

        private int i = 0 ;
        private Record next;
        private RocksIterator currentRocksIter;

        RocksDbToRecordIterator(RocksIterator rocksIterator) {
            currentRocksIter = rocksIterator;

            if (rocksIterator.isValid()) {
                next = Record.of(ByteBuffer.wrap(currentRocksIter.key()),
                        ByteBuffer.wrap(currentRocksIter.value()));
            } else
                next = null;
        }

        @Override
        public boolean hasNext() {
            return currentRocksIter.isValid() && next != null;
        }

        @Override
        public Record next() {
            Record res = next;
            if (currentRocksIter.isValid()) {
                currentRocksIter.next();
                if(currentRocksIter.isValid())
                    next = Record.of(ByteBuffer.wrap(currentRocksIter.key()),
                            ByteBuffer.wrap(currentRocksIter.value()));
                else next = null;
            } else {
                next = null;
            }
            return res;
        }


    }
}


