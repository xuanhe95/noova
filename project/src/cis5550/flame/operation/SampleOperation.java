package cis5550.flame.operation;

import cis5550.flame.FlameRDDImpl;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.KeyEncoder;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));


        log.info("[sample] input = " + ctx.input());
        log.info("[sample] output = " + ctx.output());

        String encodedFraction = req.queryParams("fraction");
        String decodedFraction = KeyEncoder.decode(encodedFraction);
        double fraction = Double.parseDouble(decodedFraction);

        Iterator<Row> it = ctx.rows();

        if (it == null) {
            log.error("[sample] No data found");
            return "OK";
        }

        AtomicInteger count = new AtomicInteger(0);

        it.forEachRemaining(row -> {
            if (Math.random() < fraction) {
                try {
                    ctx.getKVS().put(ctx.output(), row.key(), FlameRDDImpl.FLAME_RDD_VALUE, row.get(FlameRDDImpl.FLAME_RDD_VALUE));
                    count.incrementAndGet();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        log.info("[sample] Processed " + count.get() + " rows, to table: " + ctx.output());

        return "OK";
    }
}
