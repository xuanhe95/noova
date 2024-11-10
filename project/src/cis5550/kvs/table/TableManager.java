package cis5550.kvs.table;

import cis5550.kvs.KVS;
import cis5550.kvs.Row;
import cis5550.kvs.Worker;
import cis5550.kvs.version.Version;
import cis5550.tools.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * @author Xuanhe Zhang
 */
public class TableManager implements ITableManager {

    private static final Logger log = Logger.getLogger(TableManager.class);
    public static final String PERSIST_TABLE_PREFIX = "pt-";
    private static final Map<String, Table> TABLE_MAP = new ConcurrentSkipListMap<>();


    private static String storageDir;
    private TableManager(){
        storageDir = Worker.getStorageDir();
        // scan the storage directory to load the persist tables
        scanPersistTables();
    }

    private static TableManager instance;
    public static TableManager getInstance(){
        if(instance != null){
            return instance;
        }
        log.info("TableManager instance created");
        instance = new TableManager();
        return instance;
    }

    private static synchronized void scanPersistTables(){
        log.info("scan persist tables");
        File file = new File(storageDir);
        if(!file.exists()){
            return;
        }

        File[] files = file.listFiles();
        if(files == null){
            return;
        }

        for(File f : files){
            if(f.isDirectory()){
                String tableName = f.getName();
                // only load the table if the table name starts with the prefix and the table does not exist
                if(tableName.startsWith(PERSIST_TABLE_PREFIX) && !TABLE_MAP.containsKey(tableName)){
                    log.info("load persist table: " + tableName);
                    TABLE_MAP.put(tableName, new PersistTable(tableName, storageDir));
                }
            }
        }
    }

    public Table getTable(String tableKey){
        // get the table by the table name, if the table does not exist, return null
        // this method should not have any side effect

        // since we need support persist table, we need to check if the table is a persist table
        // if it is a persist table, we need to create a new persist table
        // but we will not put the table in the table map
        if(tableKey.startsWith(PERSIST_TABLE_PREFIX)){
            if(!TABLE_MAP.containsKey(tableKey)){
                log.info("[table manager] scan persist table: " + tableKey);
                scanPersistTables();
            }
            log.info("[table manager] get persist table: " + tableKey);
            //TABLE_MAP.put(tableKey, new PersistTable(tableKey, storageDir));
        }

        return TABLE_MAP.getOrDefault(tableKey , null);
    }

    public Set<String> getTableNames(){
        scanPersistTables();
        return new HashSet<>(TABLE_MAP.keySet());
    }

    public String view(String tableKey, String startRowKey, int limit){

        Table table = getTable(tableKey);
        log.info("[view] get table: " + tableKey);
        if(table == null){
            return "[view] Table not found";
        }
        Vector<Row> sortedRows = table.getSortedRows(startRowKey, limit + 1);
        boolean hasMore = sortedRows.size() > limit;
        log.info("[view] has more: " + hasMore);
        String nextRowKey = null;
        if(hasMore){
            nextRowKey = sortedRows.lastElement().key();
            sortedRows.remove(sortedRows.size() - 1);
        }


        Set<String> columnNames = new TreeSet<>();
        log.info("[view] get all columns sorted");
        for(Row row : sortedRows){
            columnNames.addAll(row.columns());
        }


        StringBuilder builder = new StringBuilder();

        builder.append("<html><head><title>Table View</title></head><body>");
        builder.append("<h1>").append("Table: ").append("\"").append(table.key()).append("\"").append("</h1>");
        builder.append("<table border=\"1\">");
        // create the header of the table
        builder.append("<thead>");
        builder.append("<tr>");
        builder.append("<th>Row Name</th>");
        for(String columnName : columnNames){
            builder.append("<th>").append(columnName).append("</th>");
        }
        builder.append("</tr>");
        builder.append("</thead>");
        // create the body of the table
        for(Row row : sortedRows){
            builder.append("<tr>");
            builder.append("<td>").append(row.key()).append("</td>");
            for(String columnName : columnNames){
                byte[] value = row.getBytes(columnName);
                if(value == null){
                    builder.append("<td></td>");
                } else {
                    builder.append("<td>").append(new String(value)).append("</td>");
                }
            }
            builder.append("</tr>");
        }

        builder.append("</table>");


        // create next page button
        if(hasMore){
            builder.append("<a href=\"/view/")
                    .append(tableKey)
                    .append("?fromRow=")
                    .append(nextRowKey)
                    .append("&limit=")
                    .append(limit)
                    .append("\">")
                    .append("Next Page")
                    .append("</a>");
        }

        builder.append("</body></html>");
        return builder.toString();
    }

    public String asHtml(){
        scanPersistTables();

        StringBuilder builder = new StringBuilder();
        builder.append("<html><head><title>Worker Tables</title></head><body>");
        builder.append("<h1>").append("Worker: \"").append(Worker.getId()).append("\"").append("</h1>");
        builder.append("<table border=\"1\">");
        builder.append("<tr><th>Table Name</th><th>Table Type</th><th>Row Count</th></tr>");
        for(Table table : TABLE_MAP.values()){
            builder.append("<tr>");
            // builder.append("<td>").append(table.key()).append("</td>");

            builder.append("<td>")
                    .append("<a href=\"/view/")
                    .append(table.key())
                    .append("\">")
                    .append(table.key())
                    .append("</a>")
                    .append("</td>");


            builder.append("<td>").append(table instanceof PersistTable ? "Persist" : "Transit").append("</td>");

            builder.append("<td>").append(table.count()).append("</td>");
            builder.append("</tr>");
        }
        builder.append("</table>");
        builder.append("<p><a href=\"http://").append(Worker.getCoordinatorAddr()).append("/\"").append("/>").append("Home").append("</a></p>");
        builder.append("</body></html>");
        return builder.toString();
    }

    public synchronized Table addTable(String tableKey) {
        if(tableKey == null){
            log.error("table key is null");
            return null;
        }

        // if the table already exists, return the table
        if(TABLE_MAP.containsKey(tableKey)){
            return TABLE_MAP.get(tableKey);
        }
        Table table;
        if(tableKey.startsWith(PERSIST_TABLE_PREFIX)){
            log.info("create persist table: " + tableKey);
            table = new PersistTable(tableKey, storageDir);
        } else {
            log.info("create transit table: " + tableKey);
            // create a new table if the table does not exist
            table = new TransitTable(tableKey);
        }
        TABLE_MAP.put(tableKey, table);
        return table;
    }

    public synchronized Version<Row> putValue(String tableKey, String rowKey, String columnKey, byte[] value) {
        // get the table by the table name, if the table does not exist, create it
        Table table = addTable(tableKey);
        // since this method supports versioning, it is important to create a new version of the row first.
        Version<Row> version = table.putRow(rowKey);
        Row row = version.getValue();
        log.info("current row version: " + version.getVersion());
        row.put(columnKey, value);

        log.info("putting data in table: " + tableKey + " row: " + rowKey + " column: " + columnKey);
        return version;
    }

    public byte[] getValue(String tableKey, String rowKey, String columnKey) {
        Table table = getTable(tableKey);
        if(table == null){
            log.info("table not found");
            return null;
        }

        Row row = table.getRow(rowKey).getValue();
        if(row == null){
            log.info("row not found");
            return null;
        }
        log.info("getting data in table: " + tableKey + " row: " + rowKey + " column: " + columnKey);
        return row.getBytes(columnKey);
    }

    public Version<Row> getRow(String tableKey, String rowKey, String versionKey) {
        Table table = getTable(tableKey);

        if(table == null){
            log.info("[get row] table not found");
            return null;
        }
        log.info(table.key());
        log.info("[get row] get row: " + rowKey + " version: " + versionKey);
        return table.getRow(rowKey, versionKey);
    }

//    @Override
//    public void put(String tableName, String rowName, String column, byte[] value) throws FileNotFoundException, IOException {
//        Table table = addTable(tableName);
//        if(table == null){
//            log.info("table not found");
//            return;
//        }
//        Row row = table.putRow(rowName).getValue();
//        row.put(column, value);
//        log.info("putting data in table: " + tableName + " row: " + rowName + " column: " + column);
//    }

    @Override
    public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
        Table table = addTable(tableName);
        table.putRow(row);
    }

//    @Override
//    public Row getRow(String tableName, String row) throws FileNotFoundException, IOException {
//        Table table = getTable(tableName);
//        if(table == null){
//            log.info("table not found");
//            return null;
//        }
//        return table.getRow(row).getValue();
//    }

//    public synchronized static Version<Row> getRow(String tableKey, String rowKey) {
//        Table table = getTable(tableKey);
//        if(table == null){
//            return null;
//        }
//
//        return table.getRow(rowKey);
//    }

//    @Override
//    public boolean existsRow(String tableName, String rowName) throws FileNotFoundException, IOException {
//        Table table = getTable(tableName);
//        if(table == null){
//            return false;
//        }
//        Row row = table.getRow(rowName).getValue();
//
//        return row != null;
//    }

    @Override
    public byte[] get(String tableName, String rowName, String column) throws FileNotFoundException, IOException {
        if(tableName == null || rowName == null || column == null){
            log.info("invalid input");
            return null;
        }

        Table table = getTable(tableName);
        if(table == null){
            log.info("table not found");
            return null;
        }

        Version<Row> version = table.getRow(rowName);
        if(version == null){
            log.info("row not found");
            return null;
        }

        Row row = version.getValue();
        if(row == null){
            log.info("row not found");
            return null;
        }
        log.info("getting data in table: " + tableName + " row: " + rowName + " column: " + column);
        return row.getBytes(column);
    }

//    @Override
//    public synchronized Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
//        Table table = getTable(tableName);
//        if(table == null){
//            log.info("table not found");
//            return null;
//        }
//        return table.getRows(startRow, endRowExclusive).iterator();
//    }

    @Override
    public int count(String tableName) throws FileNotFoundException, IOException {
        Table table = getTable(tableName);
        if(table == null){
            log.info("table not found");
            return 0;
        }
        return table.count();
    }

    @Override
    public boolean rename(String oldTableName, String newTableName) throws IOException {
        Table table = getTable(oldTableName);
        if(table == null){
            log.info("[rename] table not found");
            return false;
        }
        boolean ok = table.rename(newTableName);
        if(ok){
            log.info("[rename] rename table: " + oldTableName + " to " + newTableName);
            TABLE_MAP.remove(oldTableName);
            if(TABLE_MAP.containsKey(oldTableName)){
                log.info("old table name still exists");
            }
            TABLE_MAP.put(newTableName, table);
            scanPersistTables();
        }
        return ok;
    }

    @Override
    public void delete(String oldTableName) throws IOException {
        Table table = getTable(oldTableName);
        if(table == null){
            log.error("[delete] table not found");
            return;
        }
        table.clear();
        TABLE_MAP.remove(oldTableName);
    }

//    public synchronized List<Row> getRows(String tableKey, String startKey, String endKey) {
//        Table table = getTable(tableKey);
//        if(table == null){
//            return null;
//        }
//
//        return table.getRows(startKey, endKey);
//    }


    public Map<String, Integer> getRowsWithHashCodes(String tableKey, String startRowKey, String endRowKeyExclusive) {
        Table table = getTable(tableKey);
        if(table == null){
            log.info("table not found");
            return new HashMap<>();
        }
        List<Row> rows = table.getRows(startRowKey, endRowKeyExclusive);
        Map<String, Integer> rowHashCodes = new HashMap<>();
        for(Row row : rows){
            rowHashCodes.put(row.key(), row.toString().hashCode());
        }
        return rowHashCodes;
    }

//    public synchronized static void deleteTable(String tableKey) {
//        Table table = getTable(tableKey);
//        if(table == null){
//            return;
//        }
//        table.clear();
//    }


}
