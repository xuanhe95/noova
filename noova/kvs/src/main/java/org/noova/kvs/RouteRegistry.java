package org.noova.kvs;

import org.noova.kvs.NodeManager;
import org.noova.generic.node.Node;
import org.noova.kvs.replica.FetchManager;
import org.noova.kvs.replica.ReplicaManager;
import org.noova.kvs.table.Table;
import org.noova.kvs.table.TableManager;
import org.noova.kvs.table.TransitTable;
import org.noova.kvs.version.Version;
import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.http.HttpStatus;

import java.io.ByteArrayInputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RouteRegistry {

    private static final Logger log = Logger.getLogger(RouteRegistry.class);
    private static final TableManager tableManager = TableManager.getInstance();

    static void registerRenameTable() {
        // rename
        Server.put(
                "/rename/:table",
                (req, res) -> {
                    String tableKey = req.params("table");
                    String newTableKey = req.body();


                    if (tableKey.startsWith(TableManager.PERSIST_TABLE_PREFIX) != newTableKey.startsWith(TableManager.PERSIST_TABLE_PREFIX)) {
                        log.error("[rename] Cannot rename persist table to transit table");
                        res.status(HttpStatus.BAD_REQUEST.getCode(), HttpStatus.BAD_REQUEST.getMessage());
                        return null;
                    }

                    Table table = TableManager.getInstance().getTable(tableKey);
                    if (table == null) {
                        log.error("[rename] Table not found");
                        res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                        return null;
                    }

                    Table newTable = TableManager.getInstance().getTable(newTableKey);
                    if (newTable != null) {
                        log.error("[rename] New table " +  newTableKey + " already exists");
                        //res.status(HttpStatus.CONFLICT.getCode(), HttpStatus.CONFLICT.getMessage());
                        //return null;
                    }

                    boolean ok = TableManager.getInstance().rename(tableKey, newTableKey);


                    if (ok) {
                        log.info("[rename] Table " + tableKey + " renamed to " + newTableKey);
                        res.status(HttpStatus.OK.getCode(), HttpStatus.OK.getMessage());
                        return "OK";
                    } else {
                        log.error("[rename] Table rename failed");
                        res.status(HttpStatus.INTERNAL_SERVER_ERROR.getCode(), HttpStatus.INTERNAL_SERVER_ERROR.getMessage());
                        return "FAIL";
                    }
                });


    }

    static void registerDeleteTable(){
        Server.put(
                "/delete/:table",
                (req, res) -> {
                    String tableKey = req.params("table");

                    log.info("deleting table: " + tableKey);

                    Table table = TableManager.getInstance().getTable(tableKey);
                    if(table == null){
                        log.error("Table not found");
                        res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                        return null;
                    }

                    TableManager.getInstance().delete(tableKey);

                    res.status(HttpStatus.OK.getCode(), HttpStatus.OK.getMessage());
                    return "OK";
                });
    }


    static void registerPutData() {
        // put data
        Server.put(
                "/data/:table/:row/:column",
                (req, res) -> {

                    synchronized (Worker.class) {

                        String tableKey = req.params("table");
                        String rowKey = decodeRowKey(req);
                        String columnKey = req.params("column");
                        byte[] data = req.bodyAsBytes();

                        log.warn("[PUT] | " +  tableKey + " | " + rowKey + " | " + columnKey + " | " + new String(data));

                        // for EC
                        // revised from query params to params
                        String ifColumn = req.params("ifcolumn");
                        String equals = req.params("equals");

                        if (ifColumn != null && equals != null) {
                            byte[] originalValue = TableManager.getInstance().getValue(tableKey, rowKey, ifColumn);
                            if (originalValue == null) {
                                return "FAIL";
                            }
                            if (new String(originalValue).equals(equals)) {
                                log.info("updating value: " + tableKey + " row: " + rowKey + " column: " + columnKey + " data: " + new String(data));
                                TableManager.getInstance().putValue(tableKey, rowKey, columnKey, data);

                                return "OK";
                            } else {
                                return "FAIL";
                            }
                        } else {
                            Table table = tableManager.addTable(tableKey);
                            if (table instanceof TransitTable) {
                                log.info("[PUT] Transit");

                                // add the row to table, it will create a new version of row
                                Version<Row> version = table.putRow(rowKey);
                                Row row = version.getValue();
                                String ver = version.getVersion();

                                res.header("version", ver);

                                row.put(columnKey, data);

                            } else {
                                log.info("[PUT] Persist");
                                Row row = table.putRow(rowKey).getValue();

                                row.put(columnKey, data);

                                tableManager.putRow(tableKey, row);
                            }

                            return "OK";
                        }
                    }
                }
        );
    }
// Server.put("/data/:table/", (var0x, var1x) -> {
//        String var2 = var0x.params("table");
//        logger.info("Streaming PUT('" + var2 + "')");
//        createTableIfNecessary(var2);
//        Map var3 = (Map)data.get(var2);
//        ByteArrayInputStream var4 = new ByteArrayInputStream(var0x.bodyAsBytes());
//
//        while(true) {
//            Row var5 = Row.readFrom(var4);
//            if (var5 == null) {
//                return "OK";
//            }
//
//            putRow(var2, var5);
//        }
//    });
    static void registerStreamPut(){
        // put row
        Server.put(
                "/data/:table",
                (req, res) -> {
                    String tableKey = req.params("table");
                    log.info("[stream put] | " + tableKey);
                    ByteArrayInputStream in = new ByteArrayInputStream(req.bodyAsBytes());
                    while(true){
                        Row row = Row.readFrom(in);
                        if(row == null){
                            return "OK";
                        }
                        tableManager.putRow(tableKey, row);
                    }
                }
        );
    }

    static void registerGetTables(){
        // get tables

        Server.get(
                "/tables",
                (req, res) -> {
                    log.info("[get tables] getting list of workers");

                    Set<String> tableNames = tableManager.getTableNames();

                    StringBuilder builder = new StringBuilder();

                    for(String tableName : tableNames) {
                        log.info("table: " + tableName);
                        builder.append(tableName);
                        builder.append("\n");
                    }

                    res.type("text/plain");

                    return builder.toString();
                });


    }
    static void registerStreamRead(){
        Server.get(
                "/data/:table",
                (req, res) -> {
                    String tableKey = req.params("table");
                    String startRow = req.queryParams("startRow");
                    String endRowExclusive = req.queryParams("endRowExclusive");

                    log.info("[stream read] | " + tableKey + " | start: " + startRow + " | endExclusive: " + endRowExclusive);

                    Table table = tableManager.getTable(tableKey);

                    if(table == null) {
                        log.warn("[stream read] Table not found");
                        res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                        return null;
                    }

                    List<Row> rows = table.getRows(startRow, endRowExclusive);

                    StringBuilder builder = new StringBuilder();

                    for(Row row : rows) {
                        log.info("Row: " + row.key());
                        builder.append(new String(row.toByteArray()));
                        builder.append("\n");
                    }
                    builder.append("\n");

                    res.body(builder.toString());

                    return null;
                });

    }

    static void registerHashCode(){
        // get row name with hash code
        Server.get(
                "/hashcode/:table",
                (req, res) -> {
                    String tableKey = req.params("table");
                    //String versionKey = req.headers("version");

                    String startRow = req.queryParams("startRow");
                    String endRowExclusive = req.queryParams("endRowExclusive");


                    log.info("getting stream data from table: " + tableKey);

                    Map<String, Integer> rows = tableManager.getRowsWithHashCodes(tableKey, startRow, endRowExclusive);


                    StringBuilder builder = new StringBuilder();

                    for(String rowKey : rows.keySet()) {
                        log.info("Row: " + rowKey);
                        builder.append(rowKey);
                        builder.append(",");
                        builder.append(rows.get(rowKey));
                        builder.append("\n");
                    }
                    builder.append("\n");


                    return builder.toString();
                });


    }

    static void registerWholeRowRead() { Server.get(
            "/data/:table/:row",
            (req, res) -> {
                String tableKey = req.params("table");
                String rowKey = decodeRowKey(req);
                String versionKey = req.headers("version");

                log.info("getting data from table: " + tableKey + " row: " + rowKey + " version: " + versionKey);

                Version<Row> version = tableManager.getRow(tableKey, rowKey, versionKey);

                if(version == null) {
                    log.error("Version not found");
                    res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                    return null;
                }

                Row row = version.getValue();

                if(row == null) {
                    log.warn("Row not found");
                    res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                    return null;
                }

                res.header("version", version.getVersion());
                res.bodyAsBytes(row.toByteArray());
                return null;
            });
    }

    static void registerCount(){
        Server.get(
                "/count/:table",
                (req, res) -> {
                    String tableKey = req.params("table");

                    log.info("getting count of table: " + tableKey);

                    Table table = tableManager.getTable(tableKey);
                    if(table == null){
                        log.error("Table not found");
                        res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                        return null;
                    }

                    int count = table.count();

                    res.status(HttpStatus.OK.getCode(), HttpStatus.OK.getMessage());
                    return Integer.toString(count);
                });
    }

    static void registerView(){
        Server.get(
                "/view/:table",
                (req, res) -> {
                    String tableKey = req.params("table");

                    log.info("[view] | " + tableKey);

                    String fromRow = req.queryParams("fromRow");
                    String limitStr = req.queryParams("limit");
                    int limit = 10;

                    log.info("[view] fromRow: " + fromRow + " limit: " + limitStr);

                    if(limitStr != null){
                        try{
                            limit = Integer.parseInt(limitStr);
                        } catch(Exception e){
                            log.error("Error parsing limit", e);
                        }
                    }

                    if(fromRow == null){
                        fromRow = "";
                    }

                    String view = tableManager.view(tableKey, fromRow, limit);

                    res.status(HttpStatus.OK.getCode(), HttpStatus.OK.getMessage());
                    return view;
                });
    }

    static void registerGetData(){
        Server.get(
                "/data/:table/:row/:column",
                (req, res) -> {
                    String tableKey = req.params("table");
                    String rowKey = decodeRowKey(req);
                    String columnKey = req.params("column");
                    String versionKey = req.headers("version");

                    log.info("[GET] | " +  tableKey + " | " + rowKey + " | " + columnKey);
                    Version<Row> version = tableManager.getRow(tableKey, rowKey, versionKey);


                    if(version == null) {
                        log.error("Version not found");
                        res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                        return null;
                    }

                    log.info("version: " + version.getVersion());

                    Row row = version.getValue();
                    String ver = version.getVersion();

                    res.header("version", ver);

                    byte[] data = row.getBytes(columnKey);


                    if(data == null) {
                        log.warn("Data not found");
                        res.status(HttpStatus.NOT_FOUND.getCode(), HttpStatus.NOT_FOUND.getMessage());
                        return null;
                    }

                    res.bodyAsBytes(data);
                    return null;
                }
        );
    }

    static void registerFetchRow(){
        Server.get(
                "/fetch/:table/:row",
                (req, res) -> {
                    String tableKey = req.params("table");
                    String rowKey = decodeRowKey(req);
                    String id = req.queryParams("id");

                    log.info("[fetch] | " + tableKey + " | " + rowKey);

                    Node worker =  NodeManager.getWorker(id);
                    FetchManager.pushRow(worker.asIpPort(), tableKey, rowKey);

                    return "OK";
                });
    }

    static void registerFetchTable(){
        Server.get(
                "/fetch/:table",
                (req, res) -> {
                    String tableKey = req.params("table");
                    String id = req.queryParams("id");

                    log.info("[fetch] | " + tableKey);

                    Node worker =  NodeManager.getWorker(id);
                    FetchManager.pushTable(worker.asIpPort(), tableKey);

                    return "OK";
                });
    }

    static void registerCreateTable(){
        Server.put(
                "/create/:table",
                (req, res) -> {
                    String tableKey = req.params("table");

                    log.info("[create] | " + tableKey);

                    Table table = TableManager.getInstance().getTable(tableKey);
                    if(table != null){
                        log.error("Table already exists");
                        res.status(HttpStatus.CONFLICT.getCode(), HttpStatus.CONFLICT.getMessage());
                        return null;
                    }

                    TableManager.getInstance().addTable(tableKey);

                    res.status(HttpStatus.OK.getCode(), HttpStatus.OK.getMessage());
                    return "OK";
                });
    }

    static String decodeRowKey(Request req){
        return URLDecoder.decode(req.params("row"), StandardCharsets.UTF_8);
    }


}
