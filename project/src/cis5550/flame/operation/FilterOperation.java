package cis5550.flame.operation;

import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;

public class FilterOperation implements Operation {
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {
        ctx.input(req.queryParams("input"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        log.info("[filter] input = " + ctx.input());

        FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(ctx.lambda(), ctx.getJAR());

        var it = ctx.rows();

        if (it == null) {
            log.error("No data found");
            return "OK";
        }

        it.forEachRemaining(row -> {
            log.info("[filter] get value: " + row.get(FlameRDDImpl.FLAME_RDD_VALUE));
            try {
                String value = row.get(FlameRDDImpl.FLAME_RDD_VALUE);
                if (lambda.op(value)) {
                    ctx.getKVS().put(ctx.output(), row.key(), FlameRDDImpl.FLAME_RDD_VALUE, value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return "OK";
    }
}
