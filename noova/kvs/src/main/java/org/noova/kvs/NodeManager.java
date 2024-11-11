package org.noova.kvs;

import org.noova.generic.node.Node;
import org.noova.generic.node.WorkerNode;
import org.noova.kvs.replica.FetchManager;
import org.noova.kvs.replica.ReplicaManager;
import org.noova.tools.HTTP;
import org.noova.tools.Logger;
import org.noova.generic.Worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NodeManager {
    private static final Logger log = Logger.getLogger(NodeManager.class);

    protected static final SortedMap<String, Node> WORKER_NODES_TABLE = new ConcurrentSkipListMap<>();
    private static boolean running = false;
    private static final long FETCH_INTERVAL = 5000;

    public static void startNodeThread() {
        if(running){
            return;
        }

        log.info("Starting fetch thread");

        Thread thread = new Thread(() -> {
            while(true) {
                try {
                    fetchWorkers();
                    Thread.sleep(FETCH_INTERVAL);
                } catch (InterruptedException | IOException e) {
                    log.error("Error sleeping", e);
                }
            }
        });

        thread.start();

        running = true;
    }

    public static Node getWorker(String id) {
        return WORKER_NODES_TABLE.get(id);
    }

    static void fetchWorkers() throws IOException {
        // fetch the worker list from the coordinator

        log.info("Fetching...");

        HTTP.Response res = HTTP.doRequest("GET", "http://" + Worker.coordinatorAddr + "/workers", null);

        String response = new String(res.body());

        log.info("Worker list: " + response);

        String[] workers = response.split("\n");

        for(int i = 1; i < workers.length; i++) {
            String worker = workers[i];
            String[] parts = worker.split(",");
            if(parts.length != 2) {
                log.error("Invalid worker: " + worker);
                continue;
            }
            String id = parts[0].strip();
            String[] ipPort = parts[1].strip().split(":");
            if(ipPort.length != 2) {
                log.error("Invalid worker: " + worker);
                continue;
            }
            updateWorker(id, ipPort[0], Integer.parseInt(ipPort[1]));
        }
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

    public static List<Node> getLowerReplicas(){
        List<Node> replicas = new ArrayList<Node>();
        if(WORKER_NODES_TABLE.isEmpty()){
            return replicas;
        };
        List<String> keys = new ArrayList<>(WORKER_NODES_TABLE.keySet());

        log.info("Getting lower replicas");

        // get the index of the current worker
        int index = WORKER_NODES_TABLE.headMap(Worker.id).size();

        for(int i = 0; i < ReplicaManager.REPLICA_NUM; i++){
            // get the next index, wrap around if necessary
            index = (index - 1 + WORKER_NODES_TABLE.size()) % WORKER_NODES_TABLE.size();

            Node node = WORKER_NODES_TABLE.get(keys.get(index));

            // if we reach the current worker, break
            if(node.id().equals(Worker.id)){
                break;
            }
            log.info("Adding node: " + node.id());
            replicas.add(node);
        }
        return replicas;
    }

    public static List<Node> getHigherReplicas(){
        List<Node> replicas = new ArrayList<>();
        if(WORKER_NODES_TABLE.isEmpty()){
            return replicas;
        }
        List<String> keys = new ArrayList<>(WORKER_NODES_TABLE.keySet());

        log.info("Getting higher replicas");

        // get the index of the current worker
        int index = WORKER_NODES_TABLE.headMap(Worker.id).size();

        for(int i = 0; i < FetchManager.REPLICA_MAINTENANCE_NUM; i++){
            // get the next index, wrap around if necessary
            index = (index + 1) % WORKER_NODES_TABLE.size();

            Node node = WORKER_NODES_TABLE.get(keys.get(index));

            // if we reach the current worker, break
            if(node.id().equals(Worker.id)){
                break;
            }
            log.info("Adding node: " + node.id());
            replicas.add(node);
        }
        return replicas;
    }

}
