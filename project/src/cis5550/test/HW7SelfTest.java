package cis5550.test;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HW7SelfTest extends GenericTest {

  void runSetup() {
  }

  void prompt() {

    /* Ask the user to confirm that the server is running */

    System.out.println("In separate terminal windows, please run the following commands:");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Coordinator 8000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Coordinator 9000 localhost:8000");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Worker 9001 localhost:9000");
    System.out.println("... and then hit Enter in this window to continue.");
    (new Scanner(System.in)).nextLine();
  }

  void cleanup() {
  }

  void runTests(Set<String> tests) throws Exception {

    System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
    System.out.println("--------------------------------------------------------");

    if (tests.contains("count")) {
        try {
          startTest("count", "RDD.count()", 5);
          try {
            int num = 8+(new Random()).nextInt(10);
            String arg[] = new String[1+num];
            arg[0] = "count";
            for (int i=0; i<num; i++)
              arg[1+i] = randomAlphaNum(5,10);
            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(""+num)) {
                testSucceeded();
            } else {
                testFailed("We expected to get '"+num+"', but we actually got the following:\n\n"+dump(response.getBytes()));
            }
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("save")) {
        try {
          startTest("save", "RDD.saveAsTable()", 5);
          try {
            int num = 8+(new Random()).nextInt(10);
            String arg[] = new String[2+num];
            arg[0] = "save";
            arg[1] = randomAlphaNum(5,10);
            for (int i=0; i<num; i++)
              arg[2+i] = randomAlphaNum(5,10);
            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals("OK")) {
              KVSClient kvs = new KVSClient("localhost:8000");
              boolean found[] = new boolean[num];
              for (int i=0; i<num; i++)
                found[i] = false;
              int cnt = 0;
              String nullForKey = null;
              String extraValue = null;
              String exception = null;
              try {
                Iterator<Row> iter = kvs.scan(arg[1], null, null);
                while (iter.hasNext()) {
                  Row r = iter.next();
                  cnt ++;

                  String v = r.get("value");
                  if (v == null) {
                    nullForKey = r.key();
                  } else {
                    boolean isThere = false;
                    for (int i=0; i<num; i++) {
                      if (arg[2+i].equals(v) && !found[i]) {
                        found[i] = true;
                        isThere = true;
                        break;
                      }
                    }
                    if (!isThere)
                      extraValue = v;
                  }
                }
              } catch (Exception e) {
                exception = e.toString();
              }

              if (exception != null)
                testFailed("While scanning the output table '"+arg[1]+"', we got an exception: "+exception);
              else if (cnt != num)
                testFailed("We created a table with "+num+" rows, but saveAsTable() returned one with "+cnt+" rows.");
              else if (nullForKey != null)
                testFailed("In the output table '"+arg[1]+"', we found a row with key '"+nullForKey+"' that doesn't appear to have a 'value' column.");
              else if (extraValue != null)
                testFailed("In the output table '"+arg[1]+"', we found an element (or an extra copy of an element) we didn't put into the RDD: '"+extraValue+"'.");
              else
                testSucceeded();
            } else {
              testFailed("We expected to get 'OK', but we actually got the following:\n\n"+dump(response.getBytes()));
            }
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("take")) {
        try {
          startTest("take", "RDD.take()", 5);
          try {
            Random r = new Random();
            int num = 8+r.nextInt(10), takeNum = 3+r.nextInt(4);
            String arg[] = new String[2+num];
            String theUpload = "";
            arg[0] = "take";
            arg[1] = ""+takeNum;
            for (int i=0; i<num; i++) {
              arg[2+i] = randomAlphaNum(5,10);
              theUpload = theUpload + ((i>0) ? "," : "") + arg[2+i];
            }
            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            String[] pieces = response.split(",");
            if (pieces.length == takeNum) {
              boolean found[] = new boolean[num];
              for (int i=0; i<num; i++)
                found[i] = false;
              String missingPiece = null;
              for (int i=0; i<pieces.length; i++) {
                boolean isThere = false;
                for (int j=0; j<num; j++) {
                  if (arg[2+j].equals(pieces[i]) && !found[j]) {
                    found[j] = true;
                    isThere = true;
                    break;
                  }
                }
                if (!isThere)
                  missingPiece = pieces[i];
              }
              if (missingPiece == null) {
                arg[1] = "30";
                response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
                pieces = response.split(",");
                if (pieces.length == num)
                  testSucceeded();
                else
                  testFailed("We uploaded "+num+" elements to an RDD and then called take(30), so we expected to get all "+num+" elements back, but instead we got "+pieces.length+" ('"+response+"')");
              } else {
                testFailed("We uploaded "+num+" elements ('"+theUpload+"') and then called take("+arg[1]+"), but we got '"+response+"', which doesn't look like a proper subset - for instance, '"+missingPiece+"' is not in the original set");
              }
            } else {
              testFailed("We uploaded "+num+" elements to an RDD and then called take("+arg[1]+"), but we got "+pieces.length+" elements back ('"+response+"')");
            }
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("fromtable")) {
        try {
          startTest("fromtable", "Context.fromTable()", 5);
          try {
            String tableName = randomAlphaNum(5,10);
            int num = 8+(new Random()).nextInt(10);
            String elements[] = new String[num];
            KVSClient kvs = new KVSClient("localhost:8000");
            List<String> elemList = new LinkedList<String>();
            for (int i=0; i<num; i++) {
              elements[i] = randomAlphaNum(5,10);
              kvs.put(tableName, randomAlphaNum(5,10), elements[i], randomAlphaNum(5,10).getBytes());
              elemList.add(elements[i]);
            }
            Collections.sort(elemList);
            String expected = "";
            for (String s : elemList)
              expected = expected + (expected.equals("") ? "" : ",") + s;

            String arg[] = new String[] { "fromtable", tableName };
            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(expected))
              testSucceeded();
            else
              testFailed("We put "+num+" elements ('"+expected+"') into table '"+tableName+"', then called fromTable('"+tableName+"') and collected the result, but we got '"+response+"'");
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("map1")) {
        try {
          startTest("map1", "RDD.mapToPair()", 5);
          try {
            int num = 8+(new Random()).nextInt(10);
            String arg[] = new String[1+num];
            String provided = "";
            arg[0] = "map1";
            List<String> elemList = new LinkedList<String>();
            for (int i=0; i<num; i++) {
              arg[1+i] = randomAlphaNum(5,10);
              provided = provided + ((i>0) ? "," : "") + arg[1+i];
              elemList.add("("+arg[1+i].charAt(0)+","+arg[1+i].substring(1)+")");
            }
            Collections.sort(elemList);
            String expected = "";
            for (String s : elemList)
              expected = expected + (expected.equals("") ? "" : ",") + s;

            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(expected))
              testSucceeded();
            else
              testFailed("We put "+num+" elements ('"+provided+"') into an RDD and then mapped them to pairs, with the first character serving as the key and the rest as the value; we expected\n\n"+expected+"\n\nbut we got back the following:\n\n"+dump(response.getBytes()));
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("map2")) {
        try {
          startTest("map2", "PairRDD.flatMap()", 5);
          try {
            int num = 4+2*((new Random()).nextInt(5));
            String arg[] = new String[1+num];
            String provided = "";
            arg[0] = "map2";
            List<String> elemList = new LinkedList<String>();
            for (int i=0; i<num; i+=2) {
              arg[1+i] = randomAlphaNum(3,5);
              arg[2+i] = randomAlphaNum(3,5);
              provided = provided + ((i>0) ? "," : "") + "(" + arg[1+i] + "," + arg[2+i] + ")";
              elemList.add(arg[1+i]+arg[2+i]);
              elemList.add(arg[2+i]+arg[1+i]);
            }
            Collections.sort(elemList);
            String expected = "";
            for (String s : elemList)
              expected = expected + (expected.equals("") ? "" : ",") + s;

            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(expected))
              testSucceeded();
            else
              testFailed("We put "+(num/2)+" elements ('"+provided+"') into a PairRDD and then mapped them to strings that were simply concatenations of the keys and values (for each pair, first key+value then value+key). We expected to get\n\n"+expected+"\n\nbut we got back the following instead:\n\n"+dump(response.getBytes()));
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("map3")) {
        try {
          startTest("map3", "PairRDD.flatMapToPair()", 5);
          try {
            int num = 4+2*((new Random()).nextInt(5));
            String arg[] = new String[1+num];
            String provided = "";
            arg[0] = "map3";
            List<String> elemList = new LinkedList<String>();
            for (int i=0; i<num; i+=2) {
              arg[1+i] = randomAlphaNum(3,5);
              arg[2+i] = randomAlphaNum(3,5);
              provided = provided + ((i>0) ? "," : "") + "(" + arg[1+i] + "," + arg[2+i] + ")";
              elemList.add("("+arg[1+i]+","+arg[1+i].length()+")");
              elemList.add("("+arg[2+i]+","+arg[2+i].length()+")");
            }
            Collections.sort(elemList);
            String expected = "";
            for (String s : elemList)
              expected = expected + (expected.equals("") ? "" : ",") + s;

            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(expected))
              testSucceeded();
            else
              testFailed("We put "+(num/2)+" elements ('"+provided+"') into a PairRDD and then used flatMapToPair to output (key,length) and (value,length) for each. We expected to get\n\n"+expected+"\n\nbut we got back the following instead:\n\n"+dump(response.getBytes()));
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("distinct")) {
        try {
          startTest("distinct", "RDD.distinct()", 5);
          try {
            Random r = new Random();
            int numUnique = 5+r.nextInt(5);
            int numDuplicates = 4+r.nextInt(4);
            String arg[] = new String[1+numUnique+numDuplicates];
            String provided = "";
            arg[0] = "distinct";
            List<String> elemList = new LinkedList<String>();
            for (int i=0; i<numUnique; i++) {
              arg[1+i] = randomAlphaNum(5,10);
              provided = provided + ((i>0) ? "," : "") + arg[1+i];
              elemList.add(arg[1+i]);
            }
            for (int i=0; i<numDuplicates; i++) {
              arg[1+numUnique+i] = arg[1+r.nextInt(numUnique)];
              provided = provided + "," + arg[1+numUnique+i];
            }
            Collections.sort(elemList);
            String expected = "";
            for (String s : elemList)
              expected = expected + (expected.equals("") ? "" : ",") + s;

            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(expected))
              testSucceeded();
            else
              testFailed("We put "+numUnique+" unique strings and "+numDuplicates+" duplicates ('"+provided+"') into an RDD and then called distinct(). We expected to get\n\n"+expected+"\n\nbut we got back the following:\n\n"+dump(response.getBytes()));
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("join")) {
        try {
          startTest("join", "PairRDD.join()", 15);
          try {
            Random r = new Random();
            int numKeys = 4+r.nextInt(3);
            Vector<String> table1 = new Vector<String>();
            Vector<String> table2 = new Vector<String>();
            String provided1 = "", provided2 = "";
            List<String> elemList = new LinkedList<String>();
            for (int i=0; i<numKeys; i++) {
              String key = randomAlphaNum(4,6);
              int numVal1 = 1+r.nextInt(3);
              int numVal2 = 1+r.nextInt(3);
              String[] val1 = new String[numVal1];
              String[] val2 = new String[numVal2];
              for (int j=0; j<numVal1; j++) {
                val1[j] = randomAlphaNum(4,6);
                table1.add(key);
                table1.add(val1[j]);
                provided1 = provided1 + (provided1.equals("") ? "" : ",") + "(" + key + "," + val1[j] + ")";
              }
              for (int j=0; j<numVal2; j++) {
                val2[j] = randomAlphaNum(4,6);
                table2.add(key);
                table2.add(val2[j]);
                provided2 = provided2 + (provided2.equals("") ? "" : ",") + "(" + key + "," + val2[j] + ")";
              }
              for (int j=0; j<numVal1; j++)
                for (int k=0; k<numVal2; k++)
                  elemList.add("("+key+",\""+val1[j]+","+val2[k]+"\")");
            }

            String arg[] = new String[3+table1.size()+table2.size()];
            arg[0] = "join";
            arg[1] = ""+table1.size();
            arg[2] = ""+table2.size();
            for (int i=0; i<table1.size(); i++)
              arg[3+i] = table1.elementAt(i);
            for (int i=0; i<table2.size(); i++)
              arg[3+i+table1.size()] = table2.elementAt(i);

            Collections.sort(elemList);
            String expected = "";
            for (String s : elemList)
              expected = expected + (expected.equals("") ? "" : ",") + s;

            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(expected))
              testSucceeded();
            else
              testFailed("We loaded the following two lists of pairs into PairRDDs:\n\n"+provided1+"\n"+provided2+"\n\nand then called join(). We expected to get\n\n"+expected+"\n\nbut we got back the following:\n\n"+dump(response.getBytes()));
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("fold")) {
        try {
          startTest("fold", "RDD.fold()", 10);
          try {
            Random r = new Random();
            int num = 20+r.nextInt(10);
            String arg[] = new String[1+num];
            String provided = "";
            arg[0] = "fold";
            int sum = 0;
            for (int i=0; i<num; i++) {
              int n = r.nextInt(10);
              arg[1+i] = ""+n;
              provided = provided + ((i>0) ? "," : "") + arg[1+i];
              sum += n;
            }

            String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.HW7TestJob", arg);
            if (response.equals(""+sum))
              testSucceeded();
            else
              testFailed("We put "+num+" numbers ('"+provided+"') into an RDD and then called fold() to add them up. We expected the sum to be "+sum+", but we got back the following:\n\n"+dump(response.getBytes()));
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }


    if (tests.contains("filter")) {
        try {
          startTest("filter", "RDD.filter()", 10);
          try {
            Random r = new Random();
            String[] words = new String[] { "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog" };


            String filterKey= "a";

            int num = 20+r.nextInt(10);
            String arg[] = new String[1+num];
            String provided = "";
            arg[0] = filterKey;

            int expected = Arrays.stream(words).filter(s -> s.contains(filterKey)).toList().size();

            for(int i = 0; i < num; i++) {
              arg[1+i] = words[r.nextInt(words.length)];
            }

            String response = FlameSubmit.submit("localhost:9000", "tests/flame-filter.jar", "cis5550.test.FlameFilter", arg);
            String[] parts = response.split(",");

            for(String s : parts) {
              if(!s.contains(filterKey)) {
                testFailed("We expected the word to contain the filter key " + filterKey +  ", but we got back the following:\n\n"+dump(response.getBytes()));
              }
            }
            testSucceeded();
          } catch (ConnectException ce) {
            testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
          }
        } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }
    }

    if (tests.contains("cogroup")) {
      try {
        startTest("cogroup", "PairRDD.cogroup()", 10);
        try {
          Random r = new Random();

          String[] fruits = new String[] {"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "melon", "nectarine", "orange", "pear", "quince", "raspberry", "strawberry", "tangerine", "ugli"};
          String[] grades = new String[] {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
          String[] months = new String[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
          String[] numbers = new String[] {"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};
          String[] planets = new String[] {"Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"};

          Map<String, String[]> tables = new HashMap<String, String[]>();
          tables.put("fruits", fruits);
          tables.put("grades", grades);
          tables.put("months", months);
          tables.put("numbers", numbers);
          tables.put("planets", planets);

          Map<String, List<String>> group1 = new HashMap<>();
          Map<String, List<String>> group2 = new HashMap<>();

          AtomicInteger count1 = new AtomicInteger();
          AtomicInteger count2 = new AtomicInteger();

          tables.forEach((k, v) -> {
            int num1 = r.nextInt(30);
            int num2 = r.nextInt(30);

            count1.addAndGet(num1);
            count2.addAndGet(num2);


            List<String> list1 = new ArrayList<>();
            List<String> list2 = new ArrayList<>();

            for(int i = 0; i < num1; i++) {
              String value = v[r.nextInt(v.length)];
              list1.add(value);
            }
            for(int i = 0; i < num2; i++) {
              String value = v[r.nextInt(v.length)];
              list2.add(value);
            }

            group1.put(k, list1);
            group2.put(k, list2);
          });

          String args[] = new String[1 + count1.get() + count2.get()];
          args[0] = String.valueOf(count1.get());
//          System.out.println("Count1: " + count1.get());
//            System.out.println("Count2: " + count2.get());

          int index1 = 1;
          int index2 = 1 + count1.get();
          for(String key : tables.keySet()) {
            List<String> list1 = group1.get(key);
            List<String> list2 = group2.get(key);

            for(String value : list1) {
              args[index1] = key + " " + value;
              index1++;
            }
            for(String value : list2) {
              args[index2] = key + " " + value;
              index2++;
            }
          }

          String response = FlameSubmit.submit("localhost:9000", "tests/flame-cogroup.jar", "cis5550.test.FlameCogroup", args);

          String[] parts = response.split("\n");
          for(String part : parts) {
            String[] pieces = part.split("-");
            String key = pieces[0].split(":")[1].trim();
            String value = pieces[1].split(":")[1].trim();
            String[] group1Value = value.split("],\\[")[0].split(",");
            String[] group2Value = value.split("],\\[")[1].split(",");

            // remove the brackets

            group1Value[0] = group1Value[0].substring(1);
            group2Value[group2Value.length - 1] = group2Value[group2Value.length - 1].substring(0, group2Value[group2Value.length - 1].length() - 1);

            if(!group1.containsKey(key)) {
              testFailed("We expected the key to be one of the following: " + tables.keySet() + ", but we got back the following:\n\n"+dump(response.getBytes()));
            }

            if(!group2.containsKey(key)) {
              testFailed("We expected the key to be one of the following: " + tables.keySet() + ", but we got back the following:\n\n"+dump(response.getBytes()));
            }

            Map<String, Integer> countMap1 = new HashMap<>();
            Map<String, Integer> countMap2 = new HashMap<>();

            // count the number of times each value appears in the group1 and group2

            for(String v : group1Value) {
              countMap1.put(v, countMap1.getOrDefault(v, 0) + 1);
            }

            for(String v : group2Value) {
              countMap2.put(v, countMap2.getOrDefault(v, 0) + 1);
            }


            for(String v : group1.get(key)){
                if(countMap1.get(v) == null || countMap1.get(v) == 0) {
                    testFailed("We expected the value to be one of the following in group 1: " + group1.get(key) + ", but we got back the following: "+v+"\n\n"+dump(response.getBytes()));
                }
                countMap1.put(v, countMap1.get(v) - 1);
            }

            for(String v : group2.get(key)){
                if(countMap2.get(v) == null || countMap2.get(v) == 0) {
                    testFailed("We expected the value to be one of the following in group 2: " + group2.get(key) + ", but we got back the following:\n\n"+dump(response.getBytes()));
                }
                countMap2.put(v, countMap2.get(v) - 1);
            }

            // test whether the countMap1 and countMap2 are all 0

            for(Integer c : countMap1.values()) {
              if(c != 0) {
                testFailed("We expected the value to be one of the following in group 1: " + group1.get(key) + ", but we got back the following:\n\n"+dump(response.getBytes()));
              }
            }

            for(Integer c : countMap2.values()) {
              if(c != 0) {
                testFailed("We expected the value to be one of the following in group 2: " + group2.get(key) + ", but we got back the following:\n\n"+dump(response.getBytes()));
              }
            }

//            for(String v : group1Value) {
//              if(!group1.get(key).contains(v)) {
//                testFailed("We expected the value to be one of the following in group 1: " + group1.get(key) + ", but we got back the following: "+v+"\n\n"+dump(response.getBytes()));
//              }
//            }
//
//            for(String v : group2Value) {
//              if(!group2.get(key).contains(v)) {
//                testFailed("We expected the value to be one of the following in group 2: " + group2.get(key) + ", but we got back the following:\n\n"+dump(response.getBytes()));
//              }
//            }

          }

          testSucceeded();
        } catch (Exception e) {
          testFailed("An exception occurred: " + e, false);
          e.printStackTrace(System.out);
        }

      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(System.out); }

    }

    if(tests.contains("mapPartitions")) {
      try {
        startTest("mapPartitions", "RDD.mapPartitions()", 10);
        try {
          Random r = new Random();
          String[] args = new String[] {"This is a sentence",
                                        "Another sentence for test",
                                        "The last sentence"};
          
          String response = FlameSubmit.submit("localhost:9000", "tests/flame-map-partitions.jar", "cis5550.test.FlameMapPartitions", args);
          String[] words = response.split(",");
            List<String> wordList = Arrays.asList(words);
            List<String> expected = Arrays.stream(args).flatMap(s -> Arrays.stream(s.split(" "))).toList();
            if (wordList.containsAll(expected) && expected.containsAll(wordList)) {
              testSucceeded();
            } else {
              testFailed("We expected to get the following words: " + expected + ", but we got back the following:\n\n" + dump(response.getBytes()));
            }
        } catch (ConnectException ce) {
          testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
        }
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace(System.out);
      }
    }




    System.out.println("--------------------------------------------------------\n");
    if (numTestsFailed == 0) {
        System.out.println("Looks like your solution passed all of the selected tests. Congratulations!");
    } else {
        System.out.println(numTestsFailed+" test(s) failed.");
    }

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
      System.out.println("HW7 autograder v1.0 (Feb 26, 2023)");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto")) {
      tests.add("count");
      tests.add("save");
      tests.add("take");
      tests.add("fromtable");
      tests.add("map1");
      tests.add("map2");
      tests.add("map3");
      tests.add("distinct");
      tests.add("join");
      tests.add("fold");
      tests.add("filter");
      tests.add("cogroup");
      tests.add("mapPartitions");
    }

    for (int i=0; i<args.length; i++)
      if (!args[i].equals("all") && !args[i].equals("auto") && !args[i].equals("setup") && !args[i].equals("cleanup")) 
        tests.add(args[i]);

    HW7SelfTest t = new HW7SelfTest();
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
