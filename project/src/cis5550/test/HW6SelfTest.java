package cis5550.test;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;

import java.net.ConnectException;
import java.util.*;

public class HW6SelfTest extends GenericTest {

  void runSetup() {
  }

  void prompt() {

    /* Ask the user to confirm that the server is running */

    System.out.println("In separate terminal windows, please run the following commands:");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Coordinator 8000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Coordinator 9000 localhost:8000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Worker 9001 localhost:9000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Worker 9002 localhost:9000");
    System.out.println("... and then hit Enter in this window to continue.");
    (new Scanner(System.in)).nextLine();
  }

  void cleanup() {
  }

  void runTests(Set<String> tests) throws Exception {

    System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
    System.out.println("--------------------------------------------------------");

    KVSClient kvs = new KVSClient("localhost:8000");

    if (tests.contains("output")) try {
      startTest("output", "Context.output()", 5);
      try {
        int num = 3+(new Random()).nextInt(5);
        String arg[] = new String[num];
        String expected = "Worked, and the arguments are: ";
        for (int i=0; i<num; i++) {
          arg[i] = randomAlphaNum(5,10);
          expected = expected + ((i>0) ? "," : "") + arg[i];
        }
        String response = FlameSubmit.submit("localhost:9000", "tests/flame-output.jar", "cis5550.test.FlameOutput", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-output.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());
        if (response.equals(expected))
          testSucceeded();
        else
          testFailed("We expected to get '"+expected+"', but we actually got the following\n"+dump(response.getBytes()));
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("collect")) try {
      startTest("collect", "RDD.collect()", 15);
      try {
        Random r = new Random();
        int num = 20+r.nextInt(30), extra = 10;
        String arg[] = new String[num+extra];
        for (int i=0; i<num; i++) 
          arg[i] = randomAlphaNum(5,10);
        for (int i=0; i<extra; i++)
          arg[num+i] = arg[r.nextInt(num)];

        LinkedList<String> x = new LinkedList<String>();
        for (int i=0; i<(num+extra); i++)
          x.add(arg[i]);
        Collections.sort(x);
        String expected = "";
        for (String s : x) 
          expected = expected + (expected.equals("") ? "" : ",") + s;

        String response = FlameSubmit.submit("localhost:9000", "tests/flame-collect.jar", "cis5550.test.FlameCollect", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-collect.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());
        if (response.equals(expected))
          testSucceeded();
        else
          testFailed("We expected to get '"+expected+"', but we actually got the following\n"+dump(response.getBytes()));
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("flatmap")) try {
      startTest("flatmap", "RDD.flatMap()", 25);
      try {
        String[] words = new String[] { "apple", "banana", "coconut", "date", "elderberry", "fig", "guava" };
        LinkedList<String> theWords = new LinkedList<String>();
        Random r = new Random();
        int num = 5+r.nextInt(10);
        String arg[] = new String[num];
        for (int i=0; i<num; i++) {
          int nWords = 1+r.nextInt(6);
          arg[i] = "";
          for (int j=0; j<nWords; j++) {
            String w = words[r.nextInt(words.length)];
            arg[i] = arg[i] + (arg[i].equals("") ? "" : " ") + w;
            theWords.add(w);
          }
        }

        Collections.sort(theWords);

        String argsAsString = "(";
        for (int i=0; i<arg.length; i++)
          argsAsString = argsAsString + ((i>0) ? "," : "") + "'" + arg[i] + "'";
        argsAsString += ")";

        String expected = "";
        for (String s : theWords) 
          expected = expected + (expected.equals("") ? "" : ",") + s;

        String response = FlameSubmit.submit("localhost:9000", "tests/flame-flatmap.jar", "cis5550.test.FlameFlatMap", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-flatmap.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());
        if (response.equals(expected))
          testSucceeded();
        else
          testFailed("We sent "+argsAsString+" and expected to get '"+expected+"', but we actually got the following:\n\n"+dump(response.getBytes()));
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("maptopair")) try {
      startTest("maptopair", "RDD.mapToPair()", 10);
      try {
        String[] words = new String[] { "apple", "acorn", "banana", "blueberry", "coconut", "cranberry", "chestnut" };
        Random r = new Random();
        int num = 10+r.nextInt(5);
        String arg[] = new String[num];
        List<String> exp = new LinkedList<String>();
        for (int i=0; i<num; i++) {
          arg[i] = words[r.nextInt(words.length)];
          exp.add("("+arg[i].charAt(0)+","+arg[i].substring(1)+")");
        }

        Collections.sort(exp);
        String expected = "";
        for (String s : exp) 
          expected = expected + (expected.equals("") ? "" : ",") + s;

        String response = FlameSubmit.submit("localhost:9000", "tests/flame-maptopair.jar", "cis5550.test.FlameMapToPair", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-maptopair.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());
        if (response.equals(expected))
          testSucceeded();
        else
          testFailed("We expected to get '"+expected+"', but we actually got the following\n"+dump(response.getBytes()));
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("foldbykey")) try {
      startTest("foldbykey", "PairRDD.foldByKey()", 10);
      try {
        Random r = new Random();
        int num = 20+r.nextInt(5);
        String arg[] = new String[num];
        String chr = "ABC";
        int sum[] = new int[chr.length()];
        for (int i=0; i<chr.length(); i++) {
          sum[i] = r.nextInt(20);
          arg[i] = chr.charAt(i) + " " + sum[i];
        }
        for (int i=chr.length(); i<num; i++) {
          int v = r.nextInt(20);
          int which = r.nextInt(chr.length());
          sum[which] += v;
          arg[i] = chr.charAt(which) + " " + v;
        }

        String argsAsString = "(";
        for (int i=0; i<arg.length; i++)
          argsAsString = argsAsString + ((i>0) ? "," : "") + "'" + arg[i] + "'";
        argsAsString += ")";

        String expected = "";
        for (int i=0; i<chr.length(); i++) 
          expected = expected + (expected.equals("") ? "" : ",") + "(" + chr.charAt(i)+","+sum[i]+")";

        String response = FlameSubmit.submit("localhost:9000", "tests/flame-foldbykey.jar", "cis5550.test.FlameFoldByKey", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-foldbykey.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());
        if (response.equals(expected))
          testSucceeded();
        else
          testFailed("We sent "+argsAsString+" and expected to get '"+expected+"', but we actually got the following:\n\n"+dump(response.getBytes()));
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("word-count")) try {
      startTest("word-count", "Context.output()", 5);
      try {
        //String text = "The quick brown fox jumped over the lazy dog, and then the quick brown fox jumped over the lazy dog again.";

        String text = "Person A: Hi, how are you doing today?\n" +
                "Person B: I'm doing well, thank you. How about you?\n" +
                "Person A: I'm good, just a bit tired from work.\n" +
                "Person B: I understand, work has been really busy for me as well.\n" +
                "Person A: Yeah, it's been non-stop. But I'm looking forward to the weekend.\n" +
                "Person B: Same here. Do you have any plans for the weekend?\n" +
                "Person A: Not much, just planning to relax. How about you?\n" +
                "Person B: I might go hiking if the weather is good.";
        String arg[] = text.split(" ");
        String response = FlameSubmit.submit("localhost:9000", "tests/flame-wordcount.jar", "cis5550.test.FlameWordCount", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-output.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());

        System.out.println(response);
        testSucceeded();

      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }



    if (tests.contains("intersection")) try {
      startTest("intersection", "RDD.intersection()", 10);
      try {
        String[] words1 = new String[] { "apple", "acorn", "banana", "blueberry", "coconut", "cranberry", "chestnut" };
        Random r = new Random();
        int num1 = 10+r.nextInt(5);

        String[] words2 = new String[] { "apple", "banana", "coconut" };
        int num2 = 10+r.nextInt(5);


        String arg[] = new String[num1+num2+1];

        Set<String> selected1 = new TreeSet<String>();
        Set<String> selected2 = new TreeSet<String>();

        arg[0] = Integer.toString(num1);
        for (int i=0; i<num1; i++) {
          arg[i+1] = words1[r.nextInt(words1.length)];

          selected1.add(arg[i+1]);
        }
        for (int i=0; i<num2; i++) {
          arg[i+num1+1] = words2[r.nextInt(words2.length)];
          selected2.add(arg[i+num1+1]);
        }

        Set<String> intersection = new TreeSet<String>(selected1);
        intersection.retainAll(selected2);

        String expected = "";

        for (String s : intersection)
          expected = expected + (expected.equals("") ? "" : ",") + s;

        String response = FlameSubmit.submit("localhost:9000", "tests/flame-intersection.jar", "cis5550.test.FlameIntersection", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-maptopair.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());
        if (response.equals(expected))
          testSucceeded();
        else
          testFailed("We expected to get '"+expected+"', but we actually got the following\n"+dump(response.getBytes()));
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }


    if (tests.contains("sample")) try {
      startTest("sample", "RDD.sample()", 10);
      try {

       String text[] = new String[] {"This is a test",
               "How many could survive",
               "The quick brown fox",
               "Jumped over the lazy dog",
               "And then the quick brown fox",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "Jumped over the lazy dog again",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
               "The quick brown fox",
       };

       List<String> words = new LinkedList<String>();
         for (int i=0; i<text.length; i++) {
            String[] pieces = text[i].split(" ");
            for (String s : pieces)
              words.add(s);
         }


       int total =  words.size();

       double sample = 0.1;
        String arg[] = new String[words.size()+1];
        arg[0] = Double.toString(sample);
        for (int i=0; i<words.size(); i++) {
          arg[i+1] = words.get(i);
        }
        double expected = sample;

        String response = FlameSubmit.submit("localhost:9000", "tests/flame-sample.jar", "cis5550.test.FlameSample", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-maptopair.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());

        int result = Integer.parseInt(response.split("\n")[0]);
        double ratio = result * 1.0 / total;
        if(ratio > expected + 0.1 || ratio< expected - 0.1){
            testFailed("We expected to get '"+expected+"', but actually get '"+ratio+"',and we actually got the following\n"+dump(response.getBytes()));
        }
        testSucceeded();
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("group-by")) try {
      startTest("group-by", "RDD.groupBy()", 10);
      try {

        String arg[] = new String[] {"apple", "another", "banana", "ball", "blue", "coconut", "date", "elderberry", "fig", "guava"};

        Map<String, Set<String>> map = new TreeMap<>();
        for(String s: arg){
            String key = s.substring(0,1);
            if(map.containsKey(key)){
                map.get(key).add(s);
            }else{
                Set<String> set = new TreeSet<>();
                set.add(s);
                map.put(key, set);
            }
        }

        StringBuilder expected = new StringBuilder();
        for(String key: map.keySet()){
            expected.append("(").append(key).append(",[").append(String.join(",", map.get(key))).append("])\n");
        }


        String response = FlameSubmit.submit("localhost:9000", "tests/flame-groupBy.jar", "cis5550.test.FlameGroupBy", arg);
        if (response == null)
          testFailed("We submitted a job (tests/flame-maptopair.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n"+FlameSubmit.getErrorResponse());

        String[] lines = response.split("\n");
        for(String line: lines){
           String[] parts = line.split(",");
           String key = parts[0].replaceAll("[\\[\\]()]", "");
           if(!map.containsKey(key)){
                testFailed("We expected to get '"+expected.toString()+"', but we actually got the following\n"+dump(response.getBytes()));
           }
              Set<String> set = map.get(key);
                for(int i = 1; i<parts.length; i++){
                    String word = parts[i].replaceAll("[\\[\\]()]", "");
                    if(!set.contains(word)){
                        testFailed("We expected to get '"+expected.toString()+"', but we actually got the following\n"+dump(response.getBytes()));
                    }
                }
        }

          testSucceeded();

        testSucceeded();
      } catch (ConnectException ce) {
        testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }




    System.out.println("--------------------------------------------------------\n");
    if (numTestsFailed == 0)
      System.out.println("Looks like your solution passed all of the selected tests. Congratulations!");
    else
      System.out.println(numTestsFailed+" test(s) failed.");

    cleanup();
    closeOutputFile();
  }

	public static void main(String args[]) throws Exception {

    /* Make a set of enabled tests. If no command-line arguments were specified, run all tests. */

    Set<String> tests = new TreeSet<String>();
    boolean runSetup = true, runTests = true, promptUser = true, outputToFile = false, exitUponFailure = true, cleanup = true;

    if ((args.length > 0) && args[0].equals("auto")) {
      runSetup = false;
      runTests = true;
      outputToFile = true;
      exitUponFailure = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && args[0].equals("setup")) {
      runSetup = true;
      runTests = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && args[0].equals("cleanup")) {
      runSetup = false;
      runTests = false;
      promptUser = false;
      cleanup = true;
    } else if ((args.length > 0) && args[0].equals("version")) {
      System.out.println("HW6 autograder v1.0 (Feb 19, 2023)");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto")) {
      tests.add("output");
      tests.add("collect");
      tests.add("flatmap");
      tests.add("maptopair");
      tests.add("foldbykey");
      tests.add("word-count");
      tests.add("intersection");
      tests.add("sample");
       tests.add("group-by");
    }

    for (int i=0; i<args.length; i++)
      if (!args[i].equals("all") && !args[i].equals("auto") && !args[i].equals("setup") && !args[i].equals("cleanup")) 
     	  tests.add(args[i]);

    HW6SelfTest t = new HW6SelfTest();
    t.setExitUponFailure(exitUponFailure);
    if (outputToFile)
      t.outputToFile();
    if (runSetup)
      t.runSetup();
    if (promptUser)
      t.prompt();
    if (runTests)
      t.runTests(tests);
    if (cleanup)
      t.cleanup();
  }
}
