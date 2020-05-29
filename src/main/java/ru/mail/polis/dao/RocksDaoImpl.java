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
import java.util.Iterator;
import java.util.NoSuchElementException;

import static ru.mail.polis.util.Util.fromByteBufferToByteArray;

public class RocksDaoImpl implements DAO {
    private final RocksDB db;

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

    /**
     * RocksDB upsert method with Timestamp.
     *
     * @param keys   - ByteBuffer
     * @param values - ByteBuffer
     * @throws IOException may be throw IOException
     */
    public void upsertWithTimestamp(final ByteBuffer keys, final ByteBuffer values) throws IOException {
        try {
            db.put(getKeyByteBuffer(keys),
                    Timestamp.getPresentTimestamp(System.currentTimeMillis(), values).timestampToBytes());
        } catch (RocksDBException e) {
            throw new IOException("upsertWithTimestamp", e);
        }
    }

    /**
     * RocksDB get method with Timestamp.
     *
     * @param keys - ByteBuffer
     * @return new Timestamp
     * @throws IOException            may be throw IOException
     * @throws NoSuchElementException may be throw NoSuchElementException
     */
    public Timestamp getWithTimestamp(final ByteBuffer keys) throws IOException, NoSuchElementException {
        try {
            return Timestamp.getTimestampFromBytes(db.get(getKeyByteBuffer(keys)));
        } catch (RocksDBException e) {
            throw new IOException("getWithTimestamp", e);
        }
    }

    /**
     * RocksDB remove method with Timestamp.
     *
     * @param key - ByteBuffer
     * @throws IOException may be throw IOException
     */
    public void removeWithTimestamp(final ByteBuffer key) throws IOException {
        try {
            db.put(getKeyByteBuffer(key),
                    Timestamp.getRemovedTimestamp(System.currentTimeMillis()).timestampToBytes());
        } catch (RocksDBException e) {
            throw new IOException("removeWithTimestamp", e);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final byte[] arrayKey = getKeyByteBuffer(key);
        final byte[] arrayValue = fromByteBufferToByteArray(value);
        try {
            db.put(arrayKey, arrayValue);
        } catch (RocksDBException e) {
            throw new CustomDaoException(e.getMessage(), e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final byte[] arrayKey = getKeyByteBuffer(key);
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
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        final byte[] keyArray = getKeyByteBuffer(key);
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
            throw new CustomDaoException("Compact errxcor", exception);
        }
    }

    /**
     * The method allows you to get an array of bytes from the ByteBuffer.
     *
     * @param buffer - ByteBuffer
     * @return - array of bytes
     */
    public static byte[] getArrayByte(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] body = new byte[duplicate.remaining()];

        duplicate.get(body);

        return body;
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
                final ByteBuffer value = ByteBuffer.wrap(currentRocksIter.value());
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
