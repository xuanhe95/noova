package cis5550.flame.operation;

import cis5550.flame.KeyGenerator;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JoinOperation implements Operation.PairToPair {
    private static final Logger log = Logger.getLogger(JoinOperation.class);
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {


        ctx.input(req.queryParams("input1"));
        ctx.input(req.queryParams("input2"));
        ctx.output(req.queryParams("output"));
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));


        Iterator<Row> it1 = ctx.rows(0);
        Iterator<Row> it2 = ctx.rows(1);

        if (it1 == null || it2 == null) {
            log.error("[join] No data found");
            return "No data found";
        }

        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger countCols = new AtomicInteger(0);



        Map<String, Map<String, String>> rows = new HashMap<>();

        it1.forEachRemaining(row -> {
            Map<String, String> columns = new HashMap<>();
            row.columns().forEach(column -> {
                columns.put(column, row.get(column));
            });
            rows.put(row.key(), columns);
        });
        it2.forEachRemaining(row -> {
            Map<String, String> columnsMap = rows.getOrDefault(row.key(), null);
            if (columnsMap != null) {
                row.columns().forEach(
                        column ->{
                            String value2 = row.get(column);
                            columnsMap.forEach(
                                    (key, value1) -> {
                                        String columnKey = KeyGenerator.get();
                                        try {
                                            countCols.incrementAndGet();
                                            ctx.getKVS().put(ctx.output(), row.key(), columnKey, value1+","+value2);
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                            );
                        }
                );
            }
        });


        log.info("[join] Processed " + count.get() + " rows, to table: " + ctx.output());
        log.info("[join] Processed " + countCols.get() + " columns, to table: " + ctx.output());

        return "OK";
    }


}
