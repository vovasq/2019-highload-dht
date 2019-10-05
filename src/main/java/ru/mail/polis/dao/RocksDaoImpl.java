package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static ru.mail.polis.util.Util.fromByteBufferToByteArray;

public class RocksDaoImpl implements DAO {
    private RocksDB db;


    RocksDaoImpl(@NotNull File data) throws IOException{
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try {
            final Options options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(new BytewiseComparator(new ComparatorOptions()));
            db = RocksDB.open(options, data.getPath());
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final byte[] arrayFrom = fromByteBufferToByteArray(from);
        RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seek(arrayFrom);
        return new RocksDbToRecordIterator(rocksIterator);
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        final byte[] arrayKey = fromByteBufferToByteArray(key);
        final byte[] arrayValue = fromByteBufferToByteArray(value);
        try {
            db.put(arrayKey, arrayValue);
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        final byte[] arrayKey = fromByteBufferToByteArray(key);
        try {
            db.delete(arrayKey);
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        db.close();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final byte[] keyArray = fromByteBufferToByteArray(key);
        try {
            final byte[] valueByteArray = db.get(keyArray);
            if (valueByteArray == null) {
                throw new CustomNoSuchElementException("Key not found " + key.toString());
            }
            return ByteBuffer.wrap(valueByteArray);
        } catch (RocksDBException e) {
             throw new CustomDaoException(e.getMessage(), e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException exception) {
            throw new CustomDaoException("Compact error", exception);
        }
    }

    private static class RocksDbToRecordIterator implements Iterator<Record> {

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
            return currentRocksIter.isValid() || next != null;
        }

        @Override
        public Record next() {
            final Record res = next;
            if (currentRocksIter.isValid()) {
                currentRocksIter.next();
                if (currentRocksIter.isValid()) {
                    final ByteBuffer key = ByteBuffer.wrap(currentRocksIter.key());
                    final ByteBuffer value = ByteBuffer.wrap(currentRocksIter.value());
                    next = Record.of(key, value);
                } else next = null;
            } else {
                next = null;
            }
            return res;
        }
    }
}
