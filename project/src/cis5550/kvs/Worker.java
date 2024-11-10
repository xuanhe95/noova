package cis5550.kvs;

import cis5550.generic.NodeManager;
import cis5550.kvs.replica.FetchManager;
import cis5550.kvs.table.TableManager;
import cis5550.tools.Logger;
import cis5550.webserver.Server;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class Worker extends cis5550.generic.Worker {

    private static final Logger log = Logger.getLogger(Worker.class);

    private static TableManager tableManager;


    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: Worker <port> <storage directory> <coordinator>");
            System.exit(1);
        }

        port = Integer.parseInt(args[0]);
        storageDir = args[1];
        coordinatorAddr = args[2];
        id = loadId(storageDir);

        tableManager = TableManager.getInstance();

        log.info("Starting worker on port " + port + " with id " + id);

        Server.port(port);

        Server.get("/", (req, res) -> {
            return tableManager.asHtml();
        });

        // put data
        RouteRegistry.registerPutData();
        // get data
        RouteRegistry.registerGetData();
        // whole row read
        RouteRegistry.registerWholeRowRead();
        // stream put
        RouteRegistry.registerStreamPut();
        // stream read
        RouteRegistry.registerStreamRead();
        // rename
        RouteRegistry.registerRenameTable();
        // delete table
        RouteRegistry.registerDeleteTable();
        // list of tables
        RouteRegistry.registerGetTables();
        // count
        RouteRegistry.registerCount();
        // view
        RouteRegistry.registerView();
        // fetch
        RouteRegistry.registerFetchRow();
        RouteRegistry.registerFetchTable();
        RouteRegistry.registerCreateTable();
        // hashcode
        RouteRegistry.registerHashCode();

        startPingThread(coordinatorAddr, id, port);
        NodeManager.startNodeThread();
        FetchManager.startFetchThread();
    }


    private static String loadId(String storageDir) {

        File storage = new File(storageDir);
        if(!storage.exists()) {
            log.warn("[kvs] Storage directory does not exist, creating...");
            boolean ok = storage.mkdirs();
            if(!ok) {
                log.error("[kvs] Error creating storage directory");
                throw new RuntimeException("Error creating storage directory");
            }
            log.warn("[kvs] Storage directory created");
        }


        File file = new File(storageDir + "/id");

        log.info("[kvs] Loading id from file: " + file.getAbsolutePath());

        if(file.exists()) {
            // read id from file
            try(Scanner scanner = new Scanner(file)) {
                // read id in first line
                return scanner.nextLine();
            } catch (Exception e) {
                // error reading id
                log.error("[kvs] Error reading id from file", e);
            }
        } else {
            // generate id and write id to file
            String id = generateId();

            try {

                log.info("[kvs] Creating id file: " + file.getAbsolutePath());
                boolean ok = file.createNewFile();
                if(!ok) {
                    log.error("[kvs] Error creating id file");
                    return null;
                }

                try(FileWriter writer = new FileWriter(file)) {
                    writer.write(id);
                }

                return id;
            } catch (Exception e) {
                log.error("[kvs] Error creating id file", e);
            }
        }
        return null;
    }

    private static String generateId() {
        Random random = new Random();
        StringBuilder builder = new StringBuilder();

        for(int i = 0; i < 5; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }

        return builder.toString();
    }

    public static TableManager getTableManager() {
        return tableManager;
    }





}
