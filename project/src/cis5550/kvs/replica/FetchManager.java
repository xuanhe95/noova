package cis5550.kvs.replica;

import cis5550.generic.NodeManager;
import cis5550.generic.Worker;
import cis5550.generic.node.Node;
import cis5550.kvs.Row;
import cis5550.kvs.table.Table;
import cis5550.kvs.table.TableManager;
import cis5550.tools.HTTP;
import cis5550.tools.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FetchManager {

    private static final Logger log = Logger.getLogger(FetchManager.class);

    public static final int REPLICA_MAINTENANCE_NUM = 2;
    private static final long MAINTENANCE_INTERVAL = 30000;

    private static boolean running = false;

    public static void startFetchThread(){
        if(running){
            log.warn("Fetch thread already running");
            return;
        }
        Thread thread = new Thread(()->{
            while(true) {
                try {
                    fetch();
                    Thread.sleep(MAINTENANCE_INTERVAL);
                } catch (InterruptedException e) {
                    log.error("fetch error");
                }
            }
        });
        running = true;
        thread.start();
    }

    private static void fetch() {
        List<Node> afterNodes = NodeManager.getHigherReplicas();

        for (Node node : afterNodes) {
            log.info("[sync] Fetching tables from " + node.asIpPort());
            try{
                HTTP.Response res = HTTP.doRequest("GET", "http://" + node.asIpPort() + "/tables", null);
                if(res.statusCode() != 200){
                    log.error("[sync] Failed to fetch tables from " + node.asIpPort());
                    continue;
                }
                String response = new String(res.body());
                if(response.isEmpty()){
                    log.info("[sync] No tables found from " + node.asIpPort());
                    continue;
                }

                log.info("[sync] Tables from " + node.asIpPort() + ": " + response);

                String[] replicaTableNames = response.split("\n");

                Set<String> localTableNames = TableManager.getInstance().getTableNames();

                for(String replicaTableName : replicaTableNames){
//                    if(!localTableNames.contains(replicaTableName)){
                        log.info("[sync] Try to fetch table " + replicaTableName);
                        fetchTable(node.asIpPort(), replicaTableName);
//                    }
                }
            } catch (IOException e){
                log.error("[sync] Failed to fetch tables from " + node.asIpPort());
                continue;
            }


        }
    }

    public static void fetchRow(String addr, String id, String tableName, String rowName) {
        // fetch row
        log.info("[fetch] row " + rowName + " from " + addr);
        String uri = "http://" + addr + "/fetch/" + tableName + "/" + rowName + "?id=" + id;
        Thread thread = new Thread(() -> {

            try {
                HTTP.doRequest("GET", uri, null);
            } catch (IOException e) {
                log.error("[fetch] Failed to fetch row " + rowName);
                throw new RuntimeException(e);
            }
        });
        thread.start();

    }

    public static void pushRow(String addr, String tableName, String rowName) {
        log.info("Pushing row " + rowName + " to " + addr);
        // push row
        TableManager tableManager = TableManager.getInstance();
        Table table = tableManager.getTable(tableName);
        Row row = table.getRow(rowName).getValue();
        if(row == null){
            log.error("Row not found");
            return;
        }
        Set<String> columns = row.columns();

        pushTable(addr, tableName);

        for(String column : columns){
            byte[] data = row.getBytes(column);

            String uri = "http://" + addr + "/replica/data/" + tableName + "/" + rowName + "/" + column;
            log.info("Pushing to " + uri);


            Thread thread = new Thread(() -> {
                try {
                    HTTP.doRequest("PUT", uri, data);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
            thread.start();
        }
    }


    public static void pushTable(String addr, String tableName) {
        log.info("Pushing table " + tableName + " to " + addr);
        // push table
        TableManager tableManager = TableManager.getInstance();
        Table table = tableManager.getTable(tableName);

        String url = "http://" + addr + "/create/table/" + tableName;
        log.info("Pushing to " + url);
        Thread thread = new Thread(()->{
            try {
                HTTP.doRequest("PUT", url, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
    }



    private static void fetchTable(String addr, String tableName) throws IOException {

            //String uri = "hashcode/" + tableName + "/?StartRow=" + startRow + "&endRowExclusive=" + endRowExclusive;
            String uri = "http://" + addr + "/hashcode/" + tableName;
            log.info("[sync] Fetching hash codes from " + uri);

            Thread thread = new Thread(()-> {

                HTTP.Response res;
                try {
                    res = HTTP.doRequest("GET", uri, null);
                } catch (IOException e) {
                    log.error("[sync] Fetch hash codes failed");
                    throw new RuntimeException(e);
                }
                if (res.statusCode() != 200) {
                    log.error("[sync] Failed to fetch hash codes from " + addr);
                    return;
                }

                String response = new String(res.body());
                log.info("[sync] Hash code response: " + response);

                String[] pairs = response.split("\n");
                Map<String, Integer> remoteHashCodes = new HashMap<>();

                for (String pair : pairs) {
                    String[] rowKeyHashCode = pair.split(",");
                    if (rowKeyHashCode.length != 2) {
                        log.error("[sync] Row key not match");
                        continue;
                    }
                    remoteHashCodes.put(rowKeyHashCode[0].trim(), Integer.valueOf(rowKeyHashCode[1].trim()));
                }

                Map<String, Integer> localHashCodes = TableManager.getInstance().getRowsWithHashCodes(tableName, "", null);

                for (String remoteRowKey : remoteHashCodes.keySet()) {
                    if (!localHashCodes.containsKey(remoteRowKey)) {
                        log.warn("[sync] Row + " + remoteRowKey + " not found locally");
                        fetchRow(addr, Worker.getId(), tableName, remoteRowKey);
                    } else if (!localHashCodes.get(remoteRowKey).equals(remoteHashCodes.get(remoteRowKey))) {
                        log.warn("[sync] Row " + remoteRowKey + " hash code not match");
                        fetchRow(addr, Worker.getId(), tableName, remoteRowKey);
                    } else {
                        log.info("[sync] Row " + remoteRowKey + " hash code match, fetch finished");
                    }
                }
            });
            thread.start();
    }
}
