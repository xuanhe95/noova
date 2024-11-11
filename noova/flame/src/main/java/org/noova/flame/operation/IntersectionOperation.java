package org.noova.flame.operation;

import org.noova.flame.KeyGenerator;
import org.noova.kvs.Row;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IntersectionOperation implements Operation{
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {

        ctx.input(req.queryParams("input1"));
        ctx.input(req.queryParams("input2"));
        ctx.output(req.queryParams("output"));
        ctx.lambda(req.bodyAsBytes());
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));


        log.info("[intersect] input1 = " + ctx.input(0));
        log.info("[intersect] input2 = " + ctx.input(1));

        Iterator<Row> it1 = ctx.rows(0);
        Iterator<Row> it2 = ctx.rows(1);

        if (it1 == null || it2 == null) {
            log.error("No data found");
            return "OK";
        }

        AtomicInteger count = new AtomicInteger(0);


        Map<String, Set<String>> map1 = new HashMap<>();
        Map<String, Set<String>> map2 = new HashMap<>();


        it1.forEachRemaining(row -> {
            for(String column : row.columns()){
                Set<String> set = map1.getOrDefault(column, new HashSet<>());
                set.add(row.get(column));
                log.info("[intersect1] get row key: " + row.key());
                log.info("[intersect1] get value: " + row.get(column));
                map1.put(row.key(), set);
            }
        });

        it2.forEachRemaining(row -> {
            for(String column : row.columns()){
                Set<String> set = map2.getOrDefault(column, new HashSet<>());
                set.add(row.get(column));
                log.info("[intersect2] get row key: " + row.key());
                log.info("[intersect2] get value: " + row.get(column));
                map2.put(row.key(), set);
            }
        });

        for(String rowKey : map1.keySet()){
            Set<String> set1 = map1.getOrDefault(rowKey, new HashSet<>());
            Set<String> set2 = map2.getOrDefault(rowKey, new HashSet<>());
            set1.retainAll(set2);
            for(String value : set1){
                try {
                    count.getAndIncrement();
                    String columnKey = KeyGenerator.get();
                    ctx.getKVS().put(ctx.output(), rowKey, columnKey, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        log.info("[intersect] Processed " + count.get() + " rows, to table: " + ctx.output());
        return "OK";
    }
}
