package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static ru.mail.polis.util.ByteUtil.fromByteBufferToByteArray;

public class RocksDaoImpl implements DAO {

    private RocksDB db;

    RocksDaoImpl(@NotNull final File data) throws IOException {
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try {
            final Options options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            db = RocksDB.open(options, data.getPath());
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final byte[] arrayFrom = getKeyByteBuffer(from);
        final RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seek(arrayFrom);
        return new RocksDbToRecordIterator(rocksIterator);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        upsertWrappedValue(key,new RocksValueWrapper(fromByteBufferToByteArray(value), System.currentTimeMillis(),false));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final byte[] arrayKey = getKeyByteBuffer(key);
        try {
            final byte[] rawValue = db.get(arrayKey);
            if (rawValue != null) {
                final RocksValueWrapper wrappedValue = new RocksValueWrapper(
                        "".getBytes(Charset.defaultCharset()),
                        System.currentTimeMillis(),
                        true);
                db.put(arrayKey, wrappedValue.getValue());
            }
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
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        RocksValueWrapper value = getWrappedValue(key);
        if(value.isDeleted())
            throw new CustomNoSuchElementException("key is not found");
        return  ByteBuffer.wrap(value.getValue());
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException exception) {
            throw new CustomDaoException("Compact errxcor", exception);
        }
    }

    @Override
    public void upsertWrappedValue(@NotNull ByteBuffer key, @NotNull RocksValueWrapper value) throws IOException {
        final byte[] arrayKey = getKeyByteBuffer(key);
        final byte[] arrayValue = value.toBytes();
        try {
            db.put(arrayKey, arrayValue);
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }

    }

    @NotNull
    @Override
    public RocksValueWrapper getWrappedValue(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final byte[] keyArray = getKeyByteBuffer(key);
        try {
            final RocksValueWrapper wrappedValue = new RocksValueWrapper(db.get(keyArray));
            final byte[] valueByteArray = wrappedValue.getValue();
            if (valueByteArray == null) {
                throw new CustomNoSuchElementException("Key not found " + key.toString());
            }
            return wrappedValue;
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }
    }

    private byte[] getKeyByteBuffer(@NotNull final ByteBuffer byteBuffer) {
        synchronized (this) {
            final byte[] array = fromByteBufferToByteArray(byteBuffer);
            for (int i = 0; i < array.length; i++) {
                array[i] -= Byte.MIN_VALUE;
            }
            return array;
        }
    }

    private static class RocksDbToRecordIterator implements Iterator<Record> {

        private final RocksIterator currentRocksIter;

        RocksDbToRecordIterator(final RocksIterator rocksIterator) {
            currentRocksIter = rocksIterator;
        }

        @Override
        public boolean hasNext() {
            return currentRocksIter.isValid();
        }

        @Override
        public Record next() {
            if (currentRocksIter.isValid()) {
                final ByteBuffer key = getKeyByteBuffer(currentRocksIter.key());
                final ByteBuffer value = ByteBuffer.wrap(new RocksValueWrapper(currentRocksIter.value()).getValue());
                final Record res = Record.of(key, value);
                currentRocksIter.next();
                return res;
            } else {
                throw new IllegalStateException("No next found");
            }
        }

        private ByteBuffer getKeyByteBuffer(@NotNull final byte[] array) {
            final byte[] bytes = array.clone();
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] += Byte.MIN_VALUE;
            }
            return ByteBuffer.wrap(bytes);
        }
    }
}
