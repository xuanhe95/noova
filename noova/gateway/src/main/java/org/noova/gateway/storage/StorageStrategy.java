package org.noova.gateway.storage;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.Iterator;

public class StorageStrategy {
    private static final Logger log = Logger.getLogger(StorageStrategy.class);

    private final KVS kvs;

    private static StorageStrategy instance = null;

    private StorageStrategy() {
        this.kvs = new KVSClient(
                PropertyLoader.getProperty("kvs.host") +
                        ":" + PropertyLoader.getProperty("kvs.port"));
    }

    public static StorageStrategy getInstance() {
        if(instance == null) {
            instance = new StorageStrategy();
        }
        return new StorageStrategy();
    }

    public KVS getKVS() {
        log.info("[io] Getting KVS");
        return kvs;
    }

    public void save (String tableName, String rowKey, String columnKey, byte[] value) throws IOException {
        log.info("[io] Inserting row: " + rowKey);
        var row = kvs.getRow(tableName, rowKey);
        if(row == null){
            log.info("[io] Row not found, creating new row: " + rowKey);
            row = new Row(rowKey);
        }
        row.put(columnKey, value);
        kvs.putRow(tableName, row);
    }

    public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws IOException {
        return kvs.scan(tableName, startRow, endRowExclusive);
    }

    public Iterator<Row> scan(String tableName) throws IOException {
        return kvs.scan(tableName, null, null);
    }

    public String get(String tableName, String rowKey, String columnKey) throws IOException {
        log.info("[io] Getting row: " + rowKey);
        var row = kvs.getRow(tableName, rowKey);
        if(row == null){
            log.error("[trie] Row not found: " + rowKey);
            return null;
        }
        return row.get(columnKey);
    }

    public boolean containsKey(String tableName, String rowKey, String columnKey) throws IOException {
        var row = kvs.getRow(tableName, rowKey);
        if(row == null){
            log.error("[trie] Row not found: " + rowKey);
            return false;
        }
        return row.get(columnKey) != null;
    }

    public boolean containsRow(String tableName, String rowKey) throws IOException {
        var row = kvs.getRow(tableName, rowKey);
        return row != null;
    }
}