package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameFilter {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        if(args == null || args.length < 1) {
            System.err.println("Syntax: FlameFilter <filterKey> <linesOfInputText>");
            return;
        }


        String key = args[0];

        LinkedList<String> list = new LinkedList<String>();
        for (int i=1; i<args.length; i++)
            list.add(args[i]);

        FlameRDD rdd = ctx.parallelize(list).filter(s -> s.contains(key));

        List<String> out = rdd.collect();
        Collections.sort(out);

        StringBuilder result = new StringBuilder();
        for (String s : out)
            result.append(result.isEmpty() ? "" : ",").append(s);

        ctx.output(result.toString());
    }
}
