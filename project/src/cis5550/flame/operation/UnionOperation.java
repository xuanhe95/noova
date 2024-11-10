package cis5550.flame.operation;

import cis5550.flame.FlameRDDImpl;
import cis5550.flame.KeyGenerator;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

public class UnionOperation implements Operation {
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {
        ctx.input(req.queryParams("input1"));
        ctx.input(req.queryParams("input2"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        log.info("[union] input1 = " + ctx.input(0));
        log.info("[union] input2 = " + ctx.input(1));

        var it1 = ctx.rows(0);
        var it2 = ctx.rows(1);

        while(it1 != null && it1.hasNext()){
            var row = it1.next();
            log.info("[union] get value: " + row.get(FlameRDDImpl.FLAME_RDD_VALUE));
            try {
                String rowKey = KeyGenerator.get();
                String value = row.get(FlameRDDImpl.FLAME_RDD_VALUE);
                ctx.getKVS().put(ctx.output(), rowKey , FlameRDDImpl.FLAME_RDD_VALUE, value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        while(it2 != null && it2.hasNext()){
            var row = it2.next();
            log.info("[union] get value: " + row.get(FlameRDDImpl.FLAME_RDD_VALUE));
            try {
                String rowKey = KeyGenerator.get();
                String value = row.get(FlameRDDImpl.FLAME_RDD_VALUE);
                ctx.getKVS().put(ctx.output(), rowKey , FlameRDDImpl.FLAME_RDD_VALUE, value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return "OK";
    }

}
