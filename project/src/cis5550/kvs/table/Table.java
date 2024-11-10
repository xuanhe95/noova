package cis5550.kvs.table;

import cis5550.kvs.Row;
import cis5550.kvs.version.Version;

import java.util.List;
import java.util.Vector;

public interface Table {
    String key();


    @Deprecated
    void putRow(String key, Row row);

    void putRow(Row row);
    Version<Row> putRow(String key);
    Version<Row> getRow(String key);
    Version<Row> getRow(String key, String version);
    // void deleteRow(String key);
    List<Row> getRows(String startKey, String endKeyExclusive);
    Vector<Row> getSortedRows(String startKey, int limit);
    int count();
    void clear();
    boolean rename(String key);
}
