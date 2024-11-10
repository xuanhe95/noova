package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.List;

public class FlameGroupBy {
	public static void run(FlameContext ctx, String args[]) throws Exception {

		if (args.length < 1) {
			System.err.println("Syntax: FlameIntersection <linesOfInputText>");
			return;
		}

		FlameRDD lines = ctx.parallelize(Arrays.asList(args));
		FlameRDD words = lines.flatMap(s -> {
			String[] pieces = s.split(" ");
			return Arrays.asList(pieces);
		});

		FlamePairRDD pairRDD = words.groupBy(s -> String.valueOf(s.charAt(0)));
		List<FlamePair> result = pairRDD.collect();

		result.sort((a, b) -> {
			if(a._1().compareTo(b._1()) == 0) {
				return a._2().compareTo(b._2());
			}
			return a._1().compareTo(b._1());
		});

		result.forEach(s -> ctx.output("(" + s._1() + ",[" + s._2() +"])" + "\n"));
	}
}