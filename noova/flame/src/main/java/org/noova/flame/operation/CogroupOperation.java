package org.noova.flame.operation;

import org.noova.flame.FlameRDDImpl;
import org.noova.kvs.Row;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CogroupOperation implements Operation {
    @Override
    public String execute(Request req, Response res, OperationContext ctx) {
        ctx.input(req.queryParams("input1"));
        ctx.input(req.queryParams("input2"));
        ctx.output(req.queryParams("output"));
        ctx.from(req.queryParams("from"));
        ctx.to(req.queryParams("to"));

        log.info("[cogroup] input1 = " + ctx.input(0));
        log.info("[cogroup] input2 = " + ctx.input(1));

        Iterator<Row> it1 = ctx.rows(0);
        Iterator<Row> it2 = ctx.rows(1);

        Map<String, String> map1 = buildLists(it1);
        Map<String, String> map2 = buildLists(it2);

        Set<String> commonKeys =  new HashSet<>(map1.keySet());
        commonKeys.retainAll(map2.keySet());

        for(String key : commonKeys){
            String output = map1.get(key) + "," + map2.get(key);

            log.info("[cogroup] common key: " + key + " output: " + output);
            try {
                ctx.getKVS().put(ctx.output(), key, FlameRDDImpl.FLAME_RDD_VALUE, output);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        AtomicInteger count1 = new AtomicInteger(0);
        AtomicInteger count2 = new AtomicInteger(0);


        map1.forEach((key, value) -> {
            if(!commonKeys.contains(key)){
                try {
                    ctx.getKVS().put(ctx.output(), key, FlameRDDImpl.FLAME_RDD_VALUE, value + ",[]");
                    count1.getAndIncrement();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        map2.forEach((key, value) -> {
            if(!commonKeys.contains(key)){
                try {
                    ctx.getKVS().put(ctx.output(), key, FlameRDDImpl.FLAME_RDD_VALUE, "[]," + value);
                    count2.getAndIncrement();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        log.info("[cogroup] Processed " + commonKeys.size() + " common keys, " + count1.get() + " keys from input1, " + count2.get() + " keys from input2, to table: " + ctx.output());

        return "OK";
    }

    private Map<String, String> buildLists(Iterator<Row> it){
        Map<String, List<String>> map = new HashMap<>();
        while(it != null && it.hasNext()){
            Row row = it.next();
            List<String> values = new ArrayList<>();
            log.info("[cogroup] get row key: " + row.key());
            row.columns().forEach(column -> {
                String value = row.get(column);
                log.info("[cogroup] get value: " + value);
                values.add(value);
            });
            map.put(row.key(), values);
        }

        Map<String, String> result = new HashMap<>();
        map.forEach((key, value) -> {
            result.put(key, buildString(value));
        });
        return result;
    }

    private String buildString(List<String> values){
        StringBuilder builder = new StringBuilder("[");
        for(int i = 0; i < values.size(); i++){
            builder.append(values.get(i));
            if(i != values.size() - 1){
                builder.append(",");
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
