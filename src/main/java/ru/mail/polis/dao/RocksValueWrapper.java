package ru.mail.polis.dao;

import ru.mail.polis.util.ByteUtil;

import java.util.Arrays;

/**
 * Class for extending stored in RocksDb value with timestamp.
 */
public class RocksValueWrapper {

    private byte[] value;
    private long timestamp;
    private boolean isDeleted;

    public RocksValueWrapper(final byte[] value, final long timestamp, final boolean wasDeleted) {
        this.value = value;
        this.timestamp = timestamp;
        this.isDeleted = wasDeleted;
    }

    public RocksValueWrapper(final byte[] wrappedValue) {
        if (wrappedValue == null || wrappedValue.length == 0 || isDeleted) {
            throw new CustomNoSuchElementException("Not found");
        }
        this.isDeleted = wrappedValue[0] != 0;
        this.timestamp = ByteUtil.bytesToLong(Arrays.copyOfRange(wrappedValue, 1, Long.BYTES + 1));

        if (!isDeleted) this.value = Arrays.copyOfRange(wrappedValue, Long.BYTES + 1, wrappedValue.length);
    }

    public byte[] toBytes() {
        final byte wasDeletedByte = (byte) (isDeleted ? 1 : 0);
        final byte[] timestampBytes = ByteUtil.longToBytes(timestamp);
        return ByteUtil.append(new byte[]{wasDeletedByte}, timestampBytes, value);
    }

    public byte[] getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

}
