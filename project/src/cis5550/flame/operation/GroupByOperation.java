package cis5550.flame.operation;

import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.flame.KeyGenerator;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupByOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));


        FlameRDD.StringToString lambda = (FlameRDD.StringToString) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());
        log.info("[group by] input = " + ctx.input());
        log.info("[group by] output = " + ctx.output());

        AtomicInteger count = new AtomicInteger(0);
        Iterator<Row> it = ctx.rows();

        if (it == null) {
            log.error("[group by] No data found");
            return "OK";
        }


        it.forEachRemaining(row -> {
            log.info("Row: " + row.key());
            row.columns().forEach(column -> {
                log.info("Column: " + column);
                try {
                    String value = row.get(column);
                    String key = lambda.op(value);
                    ctx.getKVS().put(ctx.output(), key, KeyGenerator.get(), value);
                    log.info("[group by] Key: " + key + " Value: " + value);
                } catch (Exception e) {
                    log.error("Error putting data into KVS");
                    throw new RuntimeException(e);
                }
            });
        });



//        Map<String, StringBuilder> map = new HashMap<>();
//
//
//        it.forEachRemaining(row -> {
//            log.info("Row: " + row.key());
//            row.columns().forEach(column -> {
//                try {
//                    String value = row.get(column);
//                    String key = lambda.op(value);
//                    if (map.containsKey(key)) {
//                        map.get(key).append(",").append(value);
//                    } else {
//                        map.put(key, new StringBuilder(value));
//                    }
//                } catch (Exception e) {
//                    log.error("Error putting data into KVS");
//                    throw new RuntimeException(e);
//                }
//            });
//        });
//
//        map.forEach((key, value) -> {
//            try {
//                count.incrementAndGet();
//                ctx.getKVS().put(ctx.output(), key, FlameRDDImpl.FLAME_RDD_VALUE, value.toString());
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });

        log.info("[group by] Processed " + count.get() + " keys, to table: " + ctx.output());

        return "OK";

    }
}
