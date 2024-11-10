package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameSample {
	public static void run(FlameContext ctx, String args[]) throws Exception {

		if (args.length < 3) {
			System.err.println("Syntax: FlameSample <probability> <linesOfInputText>");
			return;
		}

		try {
			LinkedList<String> list = new LinkedList<String>();
			double probability = Double.parseDouble(args[0]);
			for (int i = 1; i < args.length; i++) {
				list.add(args[i]);
			}
			FlameRDD lines = ctx.parallelize(list);

			FlameRDD words = lines.flatMap(s -> {
				String[] pieces = s.split(" ");
				return List.of(pieces);
			});

			var result = words.sample(probability).collect();
			Collections.sort(result);

			ctx.output(String.valueOf(result.size()) + "\n");

			for (int i = 0; i < result.size(); i++) {
				ctx.output(result.get(i) + (i == result.size() - 1 ? "" : ","));
			}
		} catch (NumberFormatException e) {
			System.err.println("Syntax: FlameSample <probability> <linesOfInputText>");
			return;
		}
	}
}