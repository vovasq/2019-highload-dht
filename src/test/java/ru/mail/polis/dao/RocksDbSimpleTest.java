package ru.mail.polis.dao;

import javafx.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


public class RocksDbSimpleTest {


    @Test
    public void putGetSimpleTest(@TempDir File data) throws IOException {
        DAO dao = DAOFactory.create(data);
        Pair<String, String> one = new Pair<>("aaaabbbbccccdddd", "dasdadasdsadsadsadasdasdsasdnaskdsadaxx");
        Pair<String, String> two = new Pair<>("bbbbaaaaccccdddd", "ssdsadsdnasdsdsasadkdsada");
        Pair<String, String> three = new Pair<>("ccccbbbbddddaaaa", "sdnassdsakdsadsdadasdasdsdasdasdasdaa");
//        List<Pair> pairs = new ArrayList<>(Arrays.asList(one,  three));
        List<Pair> pairs = new ArrayList<>(Arrays.asList(one, two, three));
        final NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        for (Pair<String, String> p : pairs) {
            dao.upsert(ByteBuffer.wrap(p.getKey().getBytes()), ByteBuffer.wrap(p.getValue().getBytes()));
            map.put(ByteBuffer.wrap(p.getKey().getBytes()), ByteBuffer.wrap(p.getValue().getBytes()));
        }
        Iterator<Record> iterator = dao.iterator(ByteBuffer.wrap(three.getKey().getBytes()));
        assertEquals(iterator.next().getKey(), ByteBuffer.wrap(three.getKey().getBytes()));
        assertFalse(iterator.hasNext());

        iterator = dao.iterator(map.lastKey());
        assertEquals(iterator.next().getValue(), map.get(map.lastKey()));
        assertFalse(iterator.hasNext());


//        iterator.next();
//        assertFalse(iterator.hasNext());
//        assertEquals(dao.get(ByteBuffer.wrap(key.getBytes())), ByteBuffer.wrap(value.getBytes()));
    }


    @Test
    public void putRemoveGetNullSimpleTest(@TempDir File data) throws IOException {
        final String key = "lol";
        final String value = "kekekadlskd;lsa";
        final String key1 = "lodsdsl";
        final String value1 = "kekeksdsadsadsadlskd;lsa";

        DAO dao = DAOFactory.create(data);
        dao.upsert(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()));
        dao.remove(ByteBuffer.wrap(key.getBytes()));
        dao.get(ByteBuffer.wrap(key.getBytes()));
//        assertEquals(dao.get(ByteBuffer.wrap(key.getBytes())), ByteBuffer.wrap(value.getBytes()));

//        dao.upsert(ByteBuffer.wrap(key1.getBytes()), ByteBuffer.wrap(value1.getBytes()));
    }

    @Test
    public void checkRocksDB(@TempDir File data) {
        RocksDB.loadLibrary();
        Options rockopts = new Options().setCreateIfMissing(true);
        RocksDB db = null;

        try {
            db = RocksDB.open(rockopts, data.getPath());
            db.put("a".getBytes(), "Value1".getBytes());
            db.put("b".getBytes(), "Value2".getBytes());
            db.put("x".getBytes(), "Value3".getBytes());
            RocksIterator iter = db.newIterator();
            iter.seek("x".getBytes());
            while (iter.isValid()) {
                String key = new String(iter.key(), StandardCharsets.UTF_8);
                String val = new String(iter.value(), StandardCharsets.UTF_8);
                System.out.println(key + " -> " + val);
                iter.next();
            }
            db.close();
        } catch (RocksDBException rdbe) {
            rdbe.printStackTrace(System.err);
        }
    }

}
