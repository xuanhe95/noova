package cis5550.flame.operation;

import cis5550.flame.FlamePairRDD;
import cis5550.flame.KeyGenerator;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.KeyEncoder;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FoldByKeyOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        String encodedAccumulator =  req.queryParams("accumulator");
        String accumulator = KeyEncoder.decode(encodedAccumulator);

        log.info("accumulator = " + accumulator);

        FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());

        AtomicInteger count = new AtomicInteger(0);
        Iterator<Row> it = ctx.rows();
        if(it == null){
            log.error("[fold by key] No data found");
            return "No data found";
        }
        it.forEachRemaining(row -> {
            AtomicReference<String> v1 = new AtomicReference<>(accumulator);

            log.info("Row: " + row.key());
            row.columns().forEach(column -> {
                String v2 = row.get(column);
                String result = lambda.op(v1.get(), v2);
                v1.set(result);
            });
            try {
                count.incrementAndGet();
                ctx.getKVS().put(ctx.output(), row.key(), KeyGenerator.get(), v1.get());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.info("Accumulator: " + v1.get());
        });


        log.info("[fold by key] Processed " + count.get() + " rows, to table: " + ctx.output());
        return "OK";
    }
}
