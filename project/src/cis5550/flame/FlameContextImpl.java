package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class FlameContextImpl implements FlameContext, Serializable {

    private static final Logger log = Logger.getLogger(FlameContextImpl.class);

    // in case of multiple output calls, using StringBuffer to keep the output synchronized
    private final StringBuffer output = new StringBuffer();

    private final String jarName;
    private int keyRangesPerWorker = 0;

    public FlameContextImpl(String jarName) {
        this.jarName = jarName;
    }

    private synchronized Vector<String[]> getKVSWorkers() {
        Vector<String[]> kvsWorkers = new Vector<>();
        try {
            int kvsWorkerNum = getKVS().numWorkers();
            for(int i = 0; i < kvsWorkerNum; i++) {
                String workerId = getKVS().getWorkerID(i);
                String workerAddr = getKVS().getWorkerAddress(i);

                kvsWorkers.add(new String[]{workerId, workerAddr});
                // HTTP.doRequest("POST", "http://" + worker + "/useJAR", jarName.getBytes());
            }

        } catch (IOException e) {
            log.error("Error getting workers from KVS");
        }

        kvsWorkers.sort((a, b) -> {
            return a[0].compareTo(b[0]);
        });

        return kvsWorkers;
    }

    private Vector<Partition> getPartitions(Vector<String[]> kvsWorkers, Vector<String> flameWorkers) {
        Partitioner partitioner = new Partitioner();
        partitioner.setKeyRangesPerWorker(3);
        for(int i = 0; i < kvsWorkers.size(); i++) {
            String workerId = kvsWorkers.get(i)[0];
            String workerAddr = kvsWorkers.get(i)[1];
            log.info("[partition] workerId: " + workerId + " workerAddr: " + workerAddr);
            if(i == kvsWorkers.size() - 1) {
                partitioner.addKVSWorker(workerAddr, workerId, null);
                partitioner.addKVSWorker(workerAddr, null, kvsWorkers.get(0)[0]);
            } else {
                partitioner.addKVSWorker(workerAddr, workerId, kvsWorkers.get(i + 1)[0]);
            }
        }
        for (String flameWorker : flameWorkers) {
            partitioner.addFlameWorker(flameWorker);
            log.info("[partition] flameWorker: " + flameWorker);
        }
        return partitioner.assignPartitions();
    }

//    private List<Throwable> uploadJar(Vector<Partitioner.Partition> partitions) throws IOException {
//        return parallelize(partitions, "useJAR", Files.readAllBytes(Paths.get(jarName)));
//    }
//
//    private List<Throwable> executeOperation(Vector<Partitioner.Partition> partitions, String operation, byte[] lambda, Vector<String> kvsWorkers, String output){
//        return parallelize(partitions, operation, lambda, kvsWorkers, output);
//    }

    private List<Throwable> parallelize(Vector<Partition> partitions, String uri, byte[] uploadOrNull, Vector<String[]> kvsWorkers, String input, String output, Map<String, String> queryParams) {
        Thread[] threads = new Thread[partitions.size()];
        List<Throwable> exceptions = new ArrayList<>();
        for(int i = 0; i < partitions.size(); i++){
            Partition partition = partitions.get(i);
            String flameWorker = partition.assignedFlameWorker;
            String kvsWorker = partition.kvsWorker;

            String fromKey = partition.fromKey;
            String toKeyExclusive = partition.toKeyExclusive;


            log.info("[partition] Assigning partition " + i + " to " + flameWorker + " from " + kvsWorker + " with keys " + fromKey + " to " + toKeyExclusive);

            StringBuilder query = new StringBuilder("?input=" + input
                    + "&output=" + output
                    + "&kvs=" + getKVS().getCoordinator());

            if(fromKey != null) {
                query.append("&from=").append(KeyEncoder.encode(fromKey));
            }
            if(toKeyExclusive != null) {
                query.append("&to=").append(KeyEncoder.encode(toKeyExclusive));
            }

            queryParams.forEach((k, v) -> {
                query.append("&").append(k).append("=").append(v);
            });

            log.info("Query params: " + query);

            Thread thread = new Thread(() -> {
                try {
                    HTTP.Response upload = HTTP.doRequest("POST", "http://" + flameWorker + "/useJAR", Files.readAllBytes(Paths.get(jarName)));
                    if(upload.statusCode() != 200) {
                        log.error("Error uploading JAR to " + flameWorker);
                        exceptions.add(new RuntimeException("Error uploading JAR to " + flameWorker));
                        return;
                    }

                    HTTP.Response res = HTTP.doRequest("POST", "http://" + flameWorker + uri + query, uploadOrNull);
                    log.info("[upload] http://" + flameWorker + uri + query);
                    if(res.statusCode() != 200) {
                        log.error("[upload] Error uploading data to " + flameWorker);
                        log.error("[upload] Error code: " + res.statusCode());
                        exceptions.add(new RuntimeException("Error uploading data to " + flameWorker));
                    }
                } catch (IOException e) {
                    log.error("Error uploading data to " + flameWorker);
                    exceptions.add(e);
                }
            });
            threads[i] = thread;
            thread.start();
        }
        for(Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return exceptions;
    }

    public String invokeOperation(String input, String operation, byte[] lambda, Map<String, String> queryParams) throws IOException {
        String output = KeyGenerator.getJob();

        Vector<String[]> kvsWorkers = getKVSWorkers();
        Vector<String> flameWorkers = Coordinator.getWorkers();

        //Partitioner p = new Partitioner();
        Partitioner partitioner = new Partitioner();

        if(keyRangesPerWorker < 0) {
            log.error("Invalid keyRangesPerWorker: " + keyRangesPerWorker);
            throw new RuntimeException("Invalid keyRangesPerWorker: " + keyRangesPerWorker);
        }
        log.info("[partition] keyRangesPerWorker: " + keyRangesPerWorker);
        partitioner.setKeyRangesPerWorker(keyRangesPerWorker);
        for(int i = 0; i < kvsWorkers.size(); i++) {
            String workerId = kvsWorkers.get(i)[0];
            String workerAddr = kvsWorkers.get(i)[1];
            log.info("[partition] workerId: " + workerId + " workerAddr: " + workerAddr);
            if(i == kvsWorkers.size() - 1) {
                partitioner.addKVSWorker(workerAddr, workerId, null);
                partitioner.addKVSWorker(workerAddr, null, kvsWorkers.get(0)[0]);
            } else {
                partitioner.addKVSWorker(workerAddr, workerId, kvsWorkers.get(i + 1)[0]);
            }
        }
        for (String flameWorker : flameWorkers) {
            partitioner.addFlameWorker(flameWorker);
            log.info("[partition] flameWorker: " + flameWorker);
        }
        Vector<Partition> partitions =  partitioner.assignPartitions();
        //Vector<Partition> partitions = getPartitions(kvsWorkers, flameWorkers);
        parallelize(partitions, operation, lambda, kvsWorkers, input, output, queryParams);

        return output;
    }

    @Override
    public KVSClient getKVS() {
        return Coordinator.kvs;
    }
    // When a job invokes output(), your solution should store the provided string
    // and return it in the body of the /submit response, if and when the job
    // terminates normally. If a job invokes output() more than once, the strings
    // should be concatenated. If a job never invokes output(), the body of the
    // /submit response should contain a message saying that there was no output.


    @Override
    public void output(String s) {
        log.info("[output] added: " + s);
        output.append(s);
    }

    public synchronized String getOutput() {
        if(output.isEmpty()) {
            return "No output";
        }
        return output.toString();
    }
//    Next, implement the context’s parallelize() method. This gets a list of
//    strings, which it is supposed to load into an RDD. First, pick a fresh table name – for instance, you could
//    have some kind of unique jobID (such as the time the job was started), followed by a sequence number.
//    Then, use the KVSClient to upload the strings in the list to this table; the column name should be
//    value, the value should be the string from the list, and the row key should be a random, unique string –
//    for instance, you can use Hasher to hash the strings 1, 2, 3, ..., and so on. Then create a new instance of
//    FlameRDDImpl, store the table name somewhere in that instance, and then return it.
    @Override
    public synchronized FlameRDD parallelize(List<String> list) throws Exception {
        String id = KeyGenerator.getJob();
        for(String s : list) {
            String columnKey = KeyGenerator.get();
            getKVS().put(id, columnKey, FlameRDDImpl.FLAME_RDD_VALUE, s);
        }

        FlameRDD flameRDD = new FlameRDDImpl(id,this);


        log.info("[parallelize] created FlameRDD with id: " + id);

        return flameRDD;
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = invokeOperation(tableName, "/rdd/fromTable", serializedLambda, new HashMap<String, String>());
        return new FlameRDDImpl(output, this);
    }


    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {
        this.keyRangesPerWorker = keyRangesPerWorker;
    }
}