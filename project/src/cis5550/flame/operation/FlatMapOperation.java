package cis5550.flame.operation;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatMapOperation implements Operation{
    private static final Logger log = Logger.getLogger(FlatMapOperation.class);
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

        it.forEachRemaining(row -> {

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

        it.forEachRemaining(row -> {
            try {
                    FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());
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

        log.info("[flat map] Processed " + count.get() + " rows, to table: " + ctx.output());
    }
}
