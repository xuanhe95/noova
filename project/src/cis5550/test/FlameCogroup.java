package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlameCogroup {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        if (args.length < 1) {
            System.err.println("Syntax: FlameCogroup <pLen> <linesOfInputText>");
            return;
        }

        int pLen = Integer.parseInt(args[0]);

        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        for(int i = 1; i < pLen + 1; i++) {
            list1.add(args[i]);
        }
        for(int i = pLen + 1; i < args.length; i++) {
            list2.add(args[i]);
        }

        FlamePairRDD rdd1 = ctx.parallelize(list1).mapToPair(s -> new FlamePair(s.split(" ")[0], s.split(" ")[1]));
        FlamePairRDD rdd2 = ctx.parallelize(list2).mapToPair(s -> new FlamePair(s.split(" ")[0], s.split(" ")[1]));

        rdd1.cogroup(rdd2).collect().forEach(s -> {
            ctx.output("type: " + s._1() + " - list: "+ s._2() + "\n");
        });

    }
}
