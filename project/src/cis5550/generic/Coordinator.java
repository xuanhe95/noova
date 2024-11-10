package cis5550.generic;

import cis5550.generic.node.Node;
import cis5550.generic.node.WorkerNode;
import cis5550.webserver.http.HttpStatus;
import cis5550.tools.Logger;

import cis5550.webserver.Server;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Xuanhe Zhang
 */
public class Coordinator {

    private static final Logger log = Logger.getLogger(Coordinator.class);
    private static final ConcurrentMap<String, Node> WORKER_NODES_TABLE = new ConcurrentHashMap<>();

    public static Vector<String> getWorkers(){
        // only return workers that are not expired
        removeWorkers();

        Vector<String> workers = new Vector<>();

        for(Node worker : WORKER_NODES_TABLE.values()) {
            workers.add(worker.asIpPort());
        }

        return workers;
    }


    public static String workerTable() {
        removeWorkers();
        StringBuilder table = new StringBuilder();


        table.append("<table border=\"1\">");

        table.append("<thead>" +
                "<tr>" +
                "<th>ID</th>" +
                "<th>IP</th>" +
                "<th>Port</th>" +
                "<th>Page</th>" +
                "<th>Last Updated</th>" +
                "</tr>" +
                "</thead>"
        );
        table.append("<tbody>");

        for(Node worker : WORKER_NODES_TABLE.values()) {
            if(worker.isExpired()) {
                continue;
            }
            table.append(worker.asHtml());
        }

        table.append("</tbody>");
        table.append("</table>");
        return table.toString();
    }


    public static String clientTable(){
        return workerTable();
    }

//    your coordinator should also accept GET requests for /, and such
//    requests should return a HTML page with a table that contains an entry for each active worker and lists its
//    ID, IP, and port. Each entry should have a hyperlink to http://ip:port/, where ip and port are the
//    IP and port number of the corresponding worker.

    public static void registerRoutes() {

        // create ping route
        Server.get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String rawPort = req.queryParams("port");

            if(id == null || rawPort == null) {
                //HandlerStrategy.handle(HttpStatus.BAD_REQUEST, req, (DynamicResponse) res);
                res.status(HttpStatus.BAD_REQUEST.getCode(), HttpStatus.BAD_REQUEST.getMessage());
                log.error("Missing id or port");
                return null;
            }

            int port;

            try{
                port = Integer.parseInt(rawPort);
            } catch(Exception e) {
                // HandlerStrategy.handle(HttpStatus.BAD_REQUEST, req, (DynamicResponse) res);
                res.status(HttpStatus.BAD_REQUEST.getCode(), HttpStatus.BAD_REQUEST.getMessage());
                log.error("Error parsing port", e);
                return null;
            }

            // update the workers
            updateWorker(id, req.ip(), port);
            return "OK";
        });

        // create workers route
        Server.get("/workers", (req, res) -> {
            // return the worker list
            return getWorkerList();
        });
    }

    private static String getWorkerList(){

        log.info("Getting worker list");
        // remove expired workers
        //removeWorkers();
        // build the response
        StringBuilder builder = new StringBuilder();
        // add the number of workers
        builder.append(WORKER_NODES_TABLE.size());
        builder.append("\n");


        // get current workers, format as ip:port
        for(Node node : WORKER_NODES_TABLE.values()) {
            if(node.isExpired()) {
                // skip expired workers
                // since this will be called by the workers, we will not call removeWorkers to keep the list up to date
                continue;
            }
            builder.append(node.id() + "," + node.asIpPort());
            builder.append("\n");
        }

        // remove the last newline if there are workers
        if(!WORKER_NODES_TABLE.isEmpty()){
            builder.delete(builder.length() - 1, builder.length());
        }

        return builder.toString();
    }

    private static void removeWorkers() {
        log.info("Removing workers");
        WORKER_NODES_TABLE.entrySet().removeIf(entry -> entry.getValue().isExpired());
    }



    private static void updateWorker(String id, String ip, int port) {
        // update by id
        Node worker = WORKER_NODES_TABLE.getOrDefault(id, new WorkerNode(id, ip, port));

        log.info("Updating worker: " + worker.id() + " " + worker.asIpPort());

        worker.ip(ip);
        worker.port(port);
        worker.heartbeat();

        WORKER_NODES_TABLE.put(id, worker);
    }


}
