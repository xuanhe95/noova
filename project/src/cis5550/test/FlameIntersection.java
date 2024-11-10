package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameIntersection {
	public static void run(FlameContext ctx, String args[]) throws Exception {

		if (args.length < 3) {
			System.err.println("Syntax: FlameIntersection <lengthOfFirstInputText> <linesOfInputText> <linesOfInputText>");
			return;
		}

		LinkedList<String> list1 = new LinkedList<String>();
		LinkedList<String> list2 = new LinkedList<String>();

		try {
			int length = Integer.parseInt(args[0]);
			for (int i = 1; i < length + 1; i++) {
				list1.add(args[i]);
			}
			for (int i = length + 1; i < args.length; i++) {
				list2.add(args[i]);
			}

		} catch (NumberFormatException e) {
			System.err.println("Syntax: FlameIntersection <lengthOfFirstInputText> <linesOfInputText> <linesOfInputText>");
			return;
		}
		FlameRDD lines1 = ctx.parallelize(list1);
		FlameRDD lines2 = ctx.parallelize(list2);

		FlameRDD words1 = lines1.flatMap(s -> {
			String[] pieces = s.split(" ");
			return List.of(pieces);
		});

		FlameRDD words2 = lines2.flatMap(s -> {
			String[] pieces = s.split(" ");
			return List.of(pieces);
		});


		List<String> result = words1.intersection(words2).collect();
		Collections.sort(result);
		StringBuilder output = new StringBuilder();
		for(int i = 0; i < result.size(); i++) {
			if(i != 0) {
				output.append(",");
			}
			output.append(result.get(i));
		}
		ctx.output(output.toString());
	}
}