package cis5550.flame.operation;

import cis5550.flame.FlamePair;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class MapToPairOperation implements Operation{

    public String execute(Request req, Response res, OperationContext ctx) {


        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));
        ctx.lambda(req.bodyAsBytes());


        FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(ctx.lambda, ctx.getJAR());
        Iterator<Row> it = ctx.rows();

        AtomicInteger count = new AtomicInteger(0);

        if (it == null) {
            log.error("[map to pair] No data found");
            return "No data found";
        }

        it.forEachRemaining(row -> {
            row.columns().forEach(column -> {
                try {
                    String value = row.get(column);
                    FlamePair result = lambda.op(value);

                    //String columnKey = KeyGenerator.get();
                    count.getAndIncrement();

                    // this should use original row key, since they are different rows
                    ctx.getKVS().put(ctx.output(), result._1(), row.key(), result._2());
                } catch (Exception e) {
                    log.error("Error putting data into KVS");
                    throw new RuntimeException(e);
                }
            });
        });

        log.info("[map to pair] Processed " + count.get() + " rows, to table: " + ctx.output());

        return "OK";
    }
}
