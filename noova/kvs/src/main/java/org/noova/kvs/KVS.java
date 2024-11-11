package org.noova.kvs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public interface KVS {
  void put(String tableName, String row, String column, byte value[]) throws FileNotFoundException, IOException;
  void putRow(String tableName, Row row) throws FileNotFoundException, IOException;
  Row getRow(String tableName, String row) throws FileNotFoundException, IOException;
  boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException;
  byte[] get(String tableName, String row, String column) throws FileNotFoundException, IOException;
  Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException;
  int count(String tableName) throws FileNotFoundException, IOException;
  boolean rename(String oldTableName, String newTableName) throws IOException;
  void delete(String oldTableName) throws IOException;
};