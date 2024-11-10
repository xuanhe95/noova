package cis5550.flame.operation;

import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.flame.KeyGenerator;
import cis5550.kvs.Row;
import cis5550.tools.KeyEncoder;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class FoldOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        String encodedAccumulator = req.queryParams("accumulator");
        String accumulator = KeyEncoder.decode(encodedAccumulator);

        log.info("[fold] accumulator = " + accumulator);

        FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());
        log.info("input = " + ctx.input());
        log.info("output = " + ctx.output());

        AtomicInteger count = new AtomicInteger(0);
        Iterator<Row> it = ctx.rows();

        while (it != null && it.hasNext()) {
            Row row = it.next();

            log.info("Row: " + row.key());

            accumulator = lambda.op(accumulator, row.get(FlameRDDImpl.FLAME_RDD_VALUE));
            count.incrementAndGet();
            log.info("Accumulator: " + accumulator);
        }

        try {
            ctx.getKVS().put(ctx.output(), KeyGenerator.get(), FlameRDDImpl.FLAME_RDD_VALUE, accumulator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("[fold] Processed " + count.get() + " rows, to table: " + ctx.output());
        return "OK";
    }
}
