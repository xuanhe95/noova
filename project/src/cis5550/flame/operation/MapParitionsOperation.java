package cis5550.flame.operation;

import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.flame.KeyGenerator;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.Iterator;

public class MapParitionsOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        log.info("[map partitions] input = " + ctx.input());

        FlameRDD.IteratorToIterator lambda = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());

        var it = ctx.rows();

        if (it == null) {
            log.error("No data found");
            return "OK";
        }

        try {
            Iterator<String> result = lambda.op(new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }
                @Override
                public String next() {
                    return it.next().get(FlameRDDImpl.FLAME_RDD_VALUE);
                }
            }
            );

            result.forEachRemaining(value -> {
                log.info("[map partitions] get value: " + value);
                String rowKey = KeyGenerator.get();
                try {
                    ctx.getKVS().put(ctx.output(), rowKey, FlameRDDImpl.FLAME_RDD_VALUE, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
