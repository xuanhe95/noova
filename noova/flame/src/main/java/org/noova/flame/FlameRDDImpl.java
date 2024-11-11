package org.noova.flame;

import org.noova.kvs.Row;
import org.noova.tools.KeyEncoder;
import org.noova.tools.Logger;
import org.noova.tools.Serializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FlameRDDImpl implements FlameRDD {

    private static final Logger log = Logger.getLogger(FlameRDDImpl.class);

    public static final String FLAME_RDD_VALUE = "value";
    private String id;
    private final FlameContextImpl context;

    public FlameRDDImpl(String id, FlameContext context) {
        this.id = id;
        this.context = (FlameContextImpl) context;
    }
//    Now implement the
//    collect() method in FlameRDDImpl; this should simply scan the table (using
//    KVSClient.scan()) and return a list with all the elements in the value column. Now the collect
//    test should work.

    public String getId() {
        return id;
    }


    public int count() throws Exception {
        return context.getKVS().count(id);
    }

    public void saveAsTable(String tableNameArg) throws Exception {


//        if(this.id.startsWith("pt-")){
//            tableNameArg = "pt-" + tableNameArg;
//            log.info("[save as table] Persist Table");
//        }

        log.info("[save as table] Renaming table " + id + " to " + tableNameArg);
        //context.getKVS().rename(id, tableNameArg);


//        Iterator<Row> it = context.getKVS().scan(id);
//
//        while(it != null & it.hasNext()){
//            Row row = it.next();
//            log.info("[save as table] key: " + row.key() + " value: " + row.get(FLAME_RDD_VALUE));
//            context.getKVS().put(tableNameArg, row.key(), FLAME_RDD_VALUE, row.get(FLAME_RDD_VALUE));
//        }
        context.getKVS().rename(id, tableNameArg);
        this.id = tableNameArg;
    }

    public FlameRDD distinct() throws Exception {
        String output = KeyGenerator.getJob();

        Iterator<Row> iterator = context.getKVS().scan(id);

        Set<String> set = new HashSet<>();
        while(iterator!=null && iterator.hasNext()){
            Row row = iterator.next();
            String value = row.get(FLAME_RDD_VALUE);

            // skip if already in set
            if(set.contains(value)){
                continue;
            }
            set.add(value);
            context.getKVS().put(output, row.key(), FLAME_RDD_VALUE, value);
        }
        return new FlameRDDImpl(output, context);
    }

    public void destroy() throws Exception {

    }

    public Vector<String> take(int num) throws Exception {
        Iterator<Row> iterator = context.getKVS().scan(id);
        Vector<String> list = new Vector<>();
        for(int i = 0; i < num; i++){
            if(iterator.hasNext()){
                list.add(iterator.next().get(FLAME_RDD_VALUE));
            } else{
                break;
            }
        }
        return list;
    }

    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        String encodedZeroElement = KeyEncoder.encode(zeroElement);
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("accumulator", encodedZeroElement);
        String output = context.invokeOperation(id, "/rdd/fold", Serializer.objectToByteArray(lambda), queryParams);

        Iterator<Row> it =  context.getKVS().scan(output);

        while(it != null & it.hasNext()){
            Row row = it.next();
            String value = row.get(FLAME_RDD_VALUE);
            log.info("[fold] key: " + row.key() + " value: " + value);
            zeroElement = lambda.op(zeroElement, value);
        }

        return zeroElement;
    }

    @Override
    public List<String> collect() throws Exception {
        int workers = context.getKVS().numWorkers();
        log.info("Number of workers: " + workers);
        int total= context.getKVS().count(id);
        log.info("Total rows in table: " + total);

        log.info("[collect] Collecting data from table: " + id);
        Iterator<Row> iterator = context.getKVS().scan(id);

        List<String> list = new Vector<>();

        SortedMap<String, String> map = new TreeMap<>();

        AtomicInteger count = new AtomicInteger(0);

        while(iterator!=null && iterator.hasNext()){
            Row row = iterator.next();


            count.getAndIncrement();
            list.add(row.get(FLAME_RDD_VALUE));
            if(map.containsKey(FLAME_RDD_VALUE)){
                log.info("[collect] Duplicate key: " + row.key());
            }
            map.put(row.key(), row.get(FLAME_RDD_VALUE));
        }

        log.info("[collect] Collected " + count.get() + " rows");
        log.info("[collect] Collected " + list.size() + " rows");

        return list;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = context.invokeOperation(id, "/rdd/flatMap", serializedLambda, new HashMap<String, String>());
        return new FlameRDDImpl(output, context);
    }

    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = context.invokeOperation(id, "/rdd/flatMapToPair", serializedLambda, new HashMap<String, String>());
        return new FlamePairRDDImpl(output, context);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = context.invokeOperation(id, "/rdd/mapToPair", serializedLambda, new HashMap<String, String>());
        return new FlamePairRDDImpl(output, context);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
//        Map<String, String> queryParams = new HashMap<>();
//        queryParams.put("input1", id);
//        queryParams.put("input2", ((FlameRDDImpl) r).getId());
//        String output = context.invokeOperation(id, "/rdd/intersection", null, queryParams);
//        return new FlameRDDImpl(output, context);
        FlamePairRDDImpl pair1 = (FlamePairRDDImpl) this.mapToPair(s -> new FlamePair(s, s));
        FlamePairRDDImpl pair2 = (FlamePairRDDImpl) r.mapToPair(s -> new FlamePair(s, s));

        return pair1.intersection(pair2).flatMap(pair ->{
            List<String> list = new ArrayList<>();
            list.add(pair._1());
            return list;
        });
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("fraction", KeyEncoder.encode(String.valueOf(f)));
        String output = context.invokeOperation(id, "/rdd/sample", null, queryParams);
        return new FlameRDDImpl(output, context);
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = context.invokeOperation(id, "/rdd/groupBy", serializedLambda, new HashMap<String, String>());
        FlamePairRDD groupBy = new FlamePairRDDImpl(output, context);

        return groupBy.foldByKey("", (s1, s2) -> {
            if(s1.isEmpty()){
                return s2;
            }
            return s1 + "," + s2;
        });
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = context.invokeOperation(id, "/rdd/filter", serializedLambda, new HashMap<String, String>());
        return new FlameRDDImpl(output, context);
    }
    // mapPartitions() should take a lambda that is given an Iterator<String>
    // and returns another Iterator<String>. The lambda should be invoked once
    // on each worker, with an iterator that contains the RDD elements
    // that worker is working on (see KVSClient.scan()); the elements
    // in the iterator that the lambda returns should be stored in
    // another RDD, which mapPartitions() should return. This method is
    // extra credit on HW7 and should return 'null' if you did not do this EC.
    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        String output = context.invokeOperation(id, "/rdd/mapPartitions", serializedLambda, new HashMap<String, String>());
        return new FlameRDDImpl(output, context);
    }

    public FlameRDD union(FlameRDD r) throws Exception {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("input1", id);
        queryParams.put("input2", ((FlameRDDImpl) r).getId());
        String output = context.invokeOperation(id, "/rdd/union", null, queryParams);
        return new FlameRDDImpl(output, context);
    }

}
