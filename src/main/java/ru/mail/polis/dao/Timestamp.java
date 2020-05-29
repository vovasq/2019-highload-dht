package ru.mail.polis.dao;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;



public final class Timestamp {

    private final State state;
    private final long stamp;
    private final ByteBuffer present;

    private Timestamp(final State type, final long stamp, final ByteBuffer present) {
        this.stamp = stamp;
        this.state = type;
        this.present = present;
    }

    static Timestamp getPresentTimestamp(final long timestamp, final ByteBuffer present) {
        return new Timestamp(State.PRESENT, timestamp, present);
    }

    static Timestamp getRemovedTimestamp(final long timestamp) {
        return new Timestamp(State.REMOVED, timestamp, null);
    }

    public static Timestamp getAbsentTimestamp() {
        return new Timestamp(State.ABSENT, -1, null);
    }

    public boolean isPresent() {
        return state == State.PRESENT;
    }

    public boolean isAbsent() {
        return state == State.ABSENT;
    }

    public boolean isRemoved() {
        return state == State.REMOVED;
    }

    public byte[] getPresentAsBytes() throws IOException {
        return RocksDaoImpl.getArrayByte(getPresent());
    }

    private ByteBuffer getPresent() throws IOException {
        if (!isPresent()) {
            throw new IOException("value is not present");
        }
        return present;
    }

    /**
     * Merge method for Timestamp.
     *
     * @param responses - list of Timestamp
     * @return - Timestamp
     */
    public static Timestamp merge(final List<Timestamp> responses) {
        if (responses.size() == 1) {
            return responses.get(0);
        } else {
            return responses.stream()
                    .filter(timestamp -> !timestamp.isAbsent())
                    .max(Comparator.comparingLong(timestamp -> timestamp.stamp))
                    .orElseGet(Timestamp::getAbsentTimestamp);
        }
    }

    /**
     * Packed array of bytes to Timestamp.
     *
     * @param bytes - array of byte
     * @return new Timestamp
     */
    public static Timestamp getTimestampFromBytes(final byte[] bytes) {
        if (bytes == null) {
            return getAbsentTimestamp();
        }
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final long stamp = buffer.getLong();
        final State recordType = getStateFromBytes(buffer.get());
        return new Timestamp(recordType, stamp, buffer);
    }

    /**
     * Unpacked array of bytes from Timestamp.
     *
     * @return array of bytes
     */
    public byte[] timestampToBytes() {
        final ByteBuffer byteBuffer;
        if (isPresent()) {
            byteBuffer = ByteBuffer.allocate(Long.BYTES + present.remaining() + 1);
        } else {
            byteBuffer = ByteBuffer.allocate(Long.BYTES + 1);
        }
        byteBuffer.putLong(stamp);
        byteBuffer.put(getBytesFromState());
        if (isPresent()) {
            byteBuffer.put(present.duplicate());
        }
        return byteBuffer.array();
    }

    private byte getBytesFromState() {
        switch (state) {
            case ABSENT:
                return 0;
            case PRESENT:
                return 1;
            default:
                return -1;
        }
    }

    private static State getStateFromBytes(final byte bytes) {
        switch (bytes) {
            case 0:
                return State.ABSENT;
            case 1:
                return State.PRESENT;
            default:
                return State.REMOVED;
        }
    }

    private enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }

}
