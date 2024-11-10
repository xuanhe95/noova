package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlameMapPartitions {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        if (args.length < 1) {
            System.err.println("Syntax: FlameMapPartitions <linesOfInputText>");
            return;
        }

        FlameRDD lines = ctx.parallelize(Arrays.asList(args));
        FlameRDD words = lines.mapPartitions(it -> {
            List<String> list = new ArrayList<>();
            while (it != null && it.hasNext()) {
                String s = it.next();
                String[] pieces = s.split(" ");
                list.addAll(Arrays.asList(pieces));
            }
            return list.iterator();
        });

        List<String> output = words.collect();
        StringBuilder result = new StringBuilder();
        for (String s : output) {
            result.append(result.isEmpty() ? "" : ",").append(s);
        }

        ctx.output(result.toString());
    }
}
