package cis5550.flame.operation;

import cis5550.flame.*;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;


import java.io.IOException;
import java.util.Iterator;


public class FlatMapToPairOperation implements Operation.RddToPair, Operation.PairToPair {
    private static final Logger log = Logger.getLogger(FlatMapToPairOperation.class);
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

        FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());
        Iterator<Row> it = ctx.rows();

        it.forEachRemaining(row -> {
            for (String column : row.columns()) {
                FlamePair flamePair = new FlamePair(row.key(), row.get(column));
                log.info("[flat map to pair]" + column + " " + flamePair._2());
                Iterable<FlamePair> result = null;
                try {
                    result = lambda.op(flamePair);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (result != null) {
                    result.forEach(pair -> {
                        String columnKey = KeyGenerator.get();
                        try {
                            //count.getAndIncrement();
                            ctx.getKVS().put(ctx.output(), pair._1(), columnKey, pair._2());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
        });
    }

    private void processRdd(OperationContext ctx) {

        log.info("[flat map to pair] RDD");
        FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());

        var it = ctx.rows();

        it.forEachRemaining(row -> {

            try {
                Iterable<FlamePair> result = lambda.op(row.get(FlameRDDImpl.FLAME_RDD_VALUE));
                if (result != null) {
                    result.forEach(pair -> {
                        String columnKey = KeyGenerator.get();
                            //count.getAndIncrement();

                        log.info("[flat map to pair] " + pair._1() + " " + pair._2());
                        try {
                            ctx.getKVS().put(ctx.output(), pair._1(), columnKey, pair._2());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                    });
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            log.info("[flat map to pair] RDD " + row.get(FlameRDDImpl.FLAME_RDD_VALUE));


        });

    }

}
