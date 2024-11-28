package org.noova.flame.operation;

import org.noova.flame.*;
import org.noova.kvs.Row;
import org.noova.tools.Logger;
import org.noova.tools.Serializer;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatMapParallelOperation implements Operation{
    private static final Logger log = Logger.getLogger(FlatMapParallelOperation.class);
    @Override
    public  String execute(Request req, Response res, OperationContext ctx) {
        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.pair(req.queryParams("pair") != null);
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        if(ctx.pair()){
            log.info("[flat map] RDD Pair");
            processRddPair(ctx);
        } else {
            log.info("[flat map] RDD");
            processRdd(ctx);
        }

        return "OK";
    }

    private void processRddPair(OperationContext ctx) {
        FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());
        //FlamePair pair = new FlamePair(input, row.key());

        var it = ctx.rows();

        if(it == null){
            log.error("[flat map] No data found");
            return;
        }
        AtomicInteger count = new AtomicInteger(0);

        // enable parallel processing
        List<Row> rows = new ArrayList<>();
        it.forEachRemaining(rows::add);

        rows.parallelStream().forEach(row -> {

        for (String column : row.columns()) {
            FlamePair pair = new FlamePair(row.key(), row.get(column));
            Iterable<String> result = null;
            try {
                result = lambda.op(pair);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (result != null) {
                result.forEach(value -> {
                    // notice: should this use random row key?
                    String rowKey = KeyGenerator.get();
                    try {
                        count.getAndIncrement();
                        ctx.getKVS().put(ctx.output(), rowKey, FlameRDDImpl.FLAME_RDD_VALUE, value);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        });


    }

    private void processRdd(OperationContext ctx) {
        AtomicInteger count = new AtomicInteger(0);
        var it = ctx.rows();
        if(it == null){
            log.error("[flat map] No data found");
            return;
        }

        // enable parallel processing
        List<Row> rows = new ArrayList<>();
        it.forEachRemaining(rows::add);

        rows.parallelStream().forEach(row -> {
            try {
                // Serializer issue? - ClassNotFoundException: org.noova.crawler.Crawler
                // thread local serialization
                FlameRDD.StringToIterable lambda= (FlameRDD.StringToIterable) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());

                // check if deserialization was successful
                if (lambda == null) {
                    log.error("[flat map] Serializer.java issue.");
                    return;
                }

                // process each row with lambda
                Iterable<String> result = lambda.op(row.get(FlameRDDImpl.FLAME_RDD_VALUE));
                if (result != null) {
                    result.forEach(key -> {
                        String rowKey = KeyGenerator.get();
                        try {
                            count.getAndIncrement();
                            ctx.getKVS().put(ctx.output(), rowKey, FlameRDDImpl.FLAME_RDD_VALUE, key);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

//        log.info("[flat map] Processed " + count.get() + " rows, to table: " + ctx.output());
    }
}
