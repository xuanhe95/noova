package org.noova.flame;

import org.noova.kvs.Row;
import org.noova.tools.KeyEncoder;
import org.noova.tools.Logger;
import org.noova.tools.Serializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FlamePairRDDImpl implements FlamePairRDD {

    private static final Logger log = Logger.getLogger(FlamePairRDDImpl.class);
    String id;
    FlameContextImpl context;

    boolean destroyed = false;

    public FlamePairRDDImpl(String id, FlameContext context){
        this.id = id;
        this.context = (FlameContextImpl) context;
    }

    public String getId() {
        return id;
    }


    // collect() should return a list that contains all the elements in the PairRDD.
    @Override
    public List<FlamePair> collect() throws Exception {
        checkDestroyed();

        Iterator<Row> rows = context.getKVS().scan(id);
        log.info("[collect] table " + id);
        List<FlamePair> pairs = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);

        rows.forEachRemaining(row -> {
            log.info("[collect] row: " + row.key());
            count.incrementAndGet();
            row.columns().forEach(column -> {
                log.info("[collect] adding " + row.get(column));
                pairs.add(new FlamePair(row.key(), row.get(column)));
            });
        });

        log.info("[collect] Total rows in table: " + count.get());

        return pairs;
    }


    // foldByKey() folds all the values that are associated with a given key in the
    // current PairRDD, and returns a new PairRDD with the resulting keys and values.
    // Formally, the new PairRDD should contain a pair (k,v) for each distinct key k
    // in the current PairRDD, where v is computed as follows: Let v_1,...,v_N be the
    // values associated with k in the current PairRDD (in other words, the current
    // PairRDD contains (k,v_1),(k,v_2),...,(k,v_N)). Then the provided lambda should
    // be invoked once for each v_i, with that v_i as the second argument. The first
    // invocation should use 'zeroElement' as its first argument, and each subsequent
    // invocation should use the result of the previous one. v is the result of the
    // last invocation.
    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        checkDestroyed();
        String encodedZeroElement = KeyEncoder.encode(zeroElement);
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("accumulator", encodedZeroElement);
        String nextId = context.invokeOperation(id, "/rdd/foldByKey", Serializer.objectToByteArray(lambda), queryParams);
        return new FlamePairRDDImpl(nextId, context);
    }
    // saveAsTable() should cause a table with the specified name to appear
    // in the KVS that contains the data from this PairRDD. The table should
    // have a row for each unique key in the PairRDD, and the different values
    // that are associated with this key should be in different columns. The
    // names of the columns can be anything.


    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        checkDestroyed();
        log.info("[save as table] Renaming table " + id + " to " + tableNameArg);
        context.getKVS().rename(id, tableNameArg);
        this.id = tableNameArg;
    }

    public FlamePairRDDImpl copyAsTable(String tableNameArg) throws Exception {
        checkDestroyed();
        log.info("[save as table] Saving table " + id + " to " + tableNameArg);

        Iterator<Row> rows = context.getKVS().scan(id);

        while(rows!=null && rows.hasNext()){
            Row row = rows.next();
            context.getKVS().putRow(tableNameArg, row);
        }
        return new FlamePairRDDImpl(tableNameArg, context);
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        checkDestroyed();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("pair", "true");
        String output = context.invokeOperation(id, "/rdd/flatMap", Serializer.objectToByteArray(lambda), queryParams);
        return new FlameRDDImpl(output, context);
    }

    @Override
    public FlameRDD flatMapParallel(PairToStringIterable lambda) throws Exception {
        checkDestroyed();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("pair", "true");
        String output = context.invokeOperation(id, "/rdd/flatMapParallel", Serializer.objectToByteArray(lambda), queryParams);
        return new FlameRDDImpl(output, context);
    }

    @Override
    public void destroy() throws Exception {
        checkDestroyed();
        log.warn("[destroy] Deleting table " + id);
        destroyed = true;
        context.getKVS().delete(id);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        checkDestroyed();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("pair", "true");
        String output = context.invokeOperation(id, "/rdd/flatMapToPair", Serializer.objectToByteArray(lambda), queryParams);
        return new FlamePairRDDImpl(output, context);
    }


    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        checkDestroyed();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("input1", id);
        queryParams.put("input2", ((FlamePairRDDImpl) other).id);
        String output = context.invokeOperation(id, "/rdd/join", null, queryParams);
        return new FlamePairRDDImpl(output, context);
    }

    // This method should return a new PairRDD that contains, for each key k that exists
    // in either the original RDD or in R, a pair (k,"[X],[Y]"), where X and Y are
    // comma-separated lists of the values from the original RDD and from R, respectively.
    // For instance, if the original RDD contains (fruit,apple) and (fruit,banana) and
    // R contains (fruit,cherry), (fruit,date) and (fruit,fig), the result should contain
    // a pair with key fruit and value [apple,banana],[cherry,date,fig]. This method is
    // extra credit in HW7; if you do not implement it, please return 'null'.
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        checkDestroyed();

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("input1", id);
        queryParams.put("input2", ((FlamePairRDDImpl) other).id);
        String output = context.invokeOperation(id, "/rdd/cogroup", null, queryParams);
        return new FlamePairRDDImpl(output, context);
    }

    public FlamePairRDD intersection(FlamePairRDD other) throws Exception {
        checkDestroyed();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("input1", id);
        queryParams.put("input2", ((FlamePairRDDImpl) other).id);
        String output = context.invokeOperation(id, "/rdd/intersection", null, queryParams);
        return new FlamePairRDDImpl(output, context);
    }

    private void checkDestroyed() throws Exception {
        if(destroyed){
            throw new Exception("RDD has been destroyed");
        }
    }
}
