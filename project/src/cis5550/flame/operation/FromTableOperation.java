package cis5550.flame.operation;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDDImpl;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class FromTableOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));


        FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());

        AtomicInteger count = new AtomicInteger(0);
        Iterator<Row> it = ctx.rows();

        if (it == null) {
            log.error("[from table] No data found");
            return "OK";
        }

        it.forEachRemaining(row -> {
            try {
                String value = lambda.op(row);
                if (value != null) {
                    ctx.getKVS().put(ctx.output(), row.key(), FlameRDDImpl.FLAME_RDD_VALUE, value);
                    count.incrementAndGet();
                }
            } catch (Exception e) {
                log.error("Error putting data into KVS");
                throw new RuntimeException(e);
            }
        });

        log.info("[from table] Processed " + count.get() + " rows, to table: " + ctx.output());

        return "OK";
    }
}
