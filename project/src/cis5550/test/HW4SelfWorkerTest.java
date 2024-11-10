package cis5550.test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class HW4SelfWorkerTest extends GenericTest {

  String id;

  HW4SelfWorkerTest() {
    super();
    id = null;
  }

  void runSetup2() throws Exception {
    File f = new File("__worker");
    if (!f.exists())
      f.mkdir();

    File f2 = new File("__worker"+File.separator+"id");
    if (f2.exists())
      f2.delete();

    id = null;
  }

  void runSetup1() throws Exception {
    File f = new File("__worker");
    if (!f.exists())
      f.mkdir();

    PrintWriter idOut = new PrintWriter("__worker"+File.separator+"id");
    id = randomAlphaNum(5,5);
    idOut.print(id);
    idOut.close();
  }

  void cleanup() throws Exception {
    File f2 = new File("__worker"+File.separator+"id");
    if (f2.exists())
      f2.delete();

    File f = new File("__worker");
    if (f.exists())
      f.delete();
  }

  void prompt() {
    /* Ask the user to confirm that the server is running */

    File f = new File("__worker");
    System.out.println("In two separate terminal windows, please run:");
    System.out.println("* java cis5550.kvs.Coordinator 8000");
    System.out.println("* java cis5550.kvs.Worker 8001 "+f.getAbsolutePath()+" localhost:8000");
    System.out.println("and then hit Enter in this window to continue. If the Coordinator and/or the Worker are already running, please terminate them and restart the test suite!");
    (new Scanner(System.in)).nextLine();
  }

  void runTests(Set<String> tests) throws Exception {
    System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
    System.out.println("--------------------------------------------------------");

    if (tests.contains("read-id")) try {
      setTimeoutMillis(30000);
      startTest("read-id", "Read the worker's ID (takes 20 seconds)", 5);
      Thread.sleep(20000);
      File f = new File("__worker");
      if (id == null) 
        id = (new Scanner(new File("__worker"+File.separator+"id"))).nextLine();

      Socket s = openSocket(8000);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      out.print("GET /workers HTTP/1.1\r\nHost: localhost\r\n\r\n");
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The coordinator returned a "+r.statusCode+" response to our GET /workers, but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      String[] pieces = r.body().split("\n");
      if (!pieces[0].equals("1"))
        testFailed("The coordinator did return a 200 status code to our GET /workers, but it was supposed to return a '1' in the first line, and it didn't. Here is what was in the body:\n\n"+dump(r.body)+"\nMaybe the worker didn't register properly? Check the ping thread!");
      if ((pieces.length < 2) || !pieces[1].contains(id))
        testFailed("The coordinator did return one worker in its response to our GET /workers, but the ID we had written to __worker/id ("+id+") did not appear. Here is what the coordinator sent:\n\n"+dump(r.body)+"\nMaybe the worker didn't read the ID frome the 'id' file in the storage directory - or maybe you didn't provide the correct path when you started the worker? It was supposed to be "+f.getAbsolutePath()+". Also, remember to start the worker AFTER the test suite (when you are prompted to hit Enter); if it is already running, the test suite will overwrite the ID and then expect to see the new value, but the worker won't see it.");
      s.close();

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    setTimeoutMillis(-1);

    if (tests.contains("put")) try {
      startTest("put", "Individual PUT", 5);
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String data = randomAlphaNum(200,500);
      String req = "PUT /data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();
      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("putget")) try {
      startTest("putget", "PUT a value and then GET it back", 10);
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
      String data = randomAlphaNum(10,20);
      String req = "PUT "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "GET "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
      out.flush();
      r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals(data))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("overwrite")) try {
      startTest("overwrite", "Overwrite a value", 5);
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
      String data1 = randomAlphaNum(10,20);
      String req = "PUT "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data1.length()+"\r\n\r\n"+data1);
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      String data2 = data1;
      while (data2.equals(data1))
        data2 = randomAlphaNum(10,20);

      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "PUT "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data2.length()+"\r\n\r\n"+data2);
      out.flush();
      r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "GET "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
      out.flush();
      r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (r.body().equals(data1))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the new value we had PUT ("+data2+"), and it returned the old value ("+data1+") instead.");
      if (!r.body().equals(data2))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the second string we had PUT in ("+data2+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }


    if (tests.contains("version")) {try {
      setTimeoutMillis(30000);
      startTest("v-put-get", "version put get", 10);

      String version1 = "v1";
      String table = randomAlphaNum(20, 30);
      String row = randomAlphaNum(30, 40);
        String column = randomAlphaNum(40, 50);
        String cell = "/data/" + table + "/" + row + "/" + column;
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String req = "PUT " + cell + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + version1.length() + "\r\n\r\n" + version1;
      out.print(req);
      out.flush();
      Response r = readAndCheckResponse(s, "response");

      String v1h = r.headers.get("version");
      if (r.statusCode != 200 || !"1".equals(v1h)) {
        testFailed("1 Expected version 1 in the response header but got " + v1h);
      }
      s.close();


      String version2 = "v2";
      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "PUT " + cell + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + version2.length() + "\r\n\r\n" + version2;
      out.print(req);
      out.flush();
      r = readAndCheckResponse(s, "response");

      String v2h = r.headers.get("version");
      if (r.statusCode != 200 || !"2".equals(v2h)) {
        testFailed("Expect value 1 in the response header but got " + v2h);
      }
      s.close();

      // get last version
      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "GET " + cell + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
      out.print(req);
      out.flush();
      r = readAndCheckResponse(s, "response");

      v2h = r.headers.get("version");
      if (r.statusCode != 200 || !"2".equals(v2h)) {
        testFailed("Expect value 2 in the response header but got " + v2h);
      }
      if(!r.body().equals(version2)) {
        testFailed("Expect " + version2 + " in the response body but got " + r.body());
      }
      s.close();

      String version3 = "v3";

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "PUT " + cell + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + version3.length() + "\r\n\r\n" + version3;
        out.print(req);
        out.flush();
        r = readAndCheckResponse(s, "response");

        String v3h = r.headers.get("version");
        if (r.statusCode != 200 || !"3".equals(v3h)) {
          testFailed("Expect value 3 in the response header but got " + v3h);
        }

        s.close();

        // get 1st version
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET " + cell + " HTTP/1.1\r\nHost: localhost\r\nVersion: 1\r\n\r\n";
        out.print(req);
        out.flush();
        r = readAndCheckResponse(s, "response");

        v1h = r.headers.get("version");
        if (r.statusCode != 200 || !"1".equals(v1h)) {
          testFailed("Expect 1 in the response header but got " + v1h);
        }
        if(!r.body().equals(version1)) {
          testFailed("Expect " + version1 + " in the response body but got " + r.body());
        }


        s.close();
        testSucceeded();
    } catch (Exception e) {
      testFailed("An exception occurred: " + e, false);
      e.printStackTrace();
    }
    }

    if(tests.contains("ifcolumn")) {
      try {
        startTest("ifcolumn", "PUT with ifcolumn", 5);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());

        // put a random value data1
        String column = String.valueOf(random(1, 10));
        String cell = "/data/" + randomAlphaNum(10, 20) + "/" + randomAlphaNum(10, 20) + "/" + column;
        String data1 = randomAlphaNum(10, 50);
        String req = "PUT " + cell;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + data1.length() + "\r\n\r\n" + data1);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));
        s.close();

        // generate a random value data2
        String data2 = data1;
        while (data2.equals(data1))
          data2 = randomAlphaNum(10, 20);


        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        // this is not equal, so it should not be updated
        req = "PUT " + cell + "?ifcolumn=" + column + "&equals=" + data2;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + data2.length() + "\r\n\r\n" + data2);
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals("FAIL"))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return 'FAIL', and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));
        s.close();

        // get the data1 to check
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET " + cell;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals(data1))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return the first string we had PUT in (" + data1 + "), and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));
        s.close();


        // this column is not exist, so it should return "FAIL"

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "PUT " + cell + "?ifcolumn=" + "notexist" + "&equals=" + data1;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + data2.length() + "\r\n\r\n" + data2);
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals("FAIL"))
            testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return 'FAIL', and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));
        s.close();

        // this is equal, so it should be updated
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "PUT " + cell + "?ifcolumn=" + column + "&equals=" + data1;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + data2.length() + "\r\n\r\n" + data2);
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));
        s.close();

        // get the data2 to check
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET " + cell;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");

        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals(data2))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return the second string we had PUT in (" + data2 + "), and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));

        s.close();
        testSucceeded();
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }

    if(tests.contains("not-found")){
      try{
        startTest("not-found", "GET a non-exist data", 5);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(10, 20)+"/"+randomAlphaNum(10, 20)+"/"+randomAlphaNum(10, 20);
        String req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 404)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 404 Not Found. Here is what was in the body:\n\n"+dump(r.body));
        s.close();
        testSucceeded();
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }}
    if(tests.contains("two-columns")) {
      try {
        startTest("two-columns", "PUT two columns", 5);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String column1 = String.valueOf(random(1, 10));
        String column2 = String.valueOf(random(20, 30));
        String cell1 = "/data/" + randomAlphaNum(10, 20) + "/" + randomAlphaNum(10, 20) + "/" + column1;
        String cell2 = "/data/" + randomAlphaNum(10, 20) + "/" + randomAlphaNum(10, 20) + "/" + column2;

        String data1 = randomAlphaNum(10, 50);
        String data2 = randomAlphaNum(10, 50);
        String req = "PUT " + cell1;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + data1.length() + "\r\n\r\n" + data1);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));

        String req2 = "PUT " + cell2;
        out.print(req2 + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + data2.length() + "\r\n\r\n" + data2);
        out.flush();

        Response r2 = readAndCheckResponse(s, "response");
        if (r2.statusCode != 200)
          testFailed("The server returned a " + r2.statusCode + " response to our " + req2 + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r2.body));
        if (!r2.body().equals("OK"))
          testFailed("The server did return a 200 status code to our " + req2 + ", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n" + dump(r2.body));

        s.close();


        // get data1 again
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET " + cell1;
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        if (!r.body().equals(data1))
          testFailed("The server did return a 200 status code to our " + req + ", but it was supposed to return the first string we had PUT in (" + data1 + "), and it didn't. Here is what was in the body instead:\n\n" + dump(r.body));


        testSucceeded();
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }
    if(tests.contains("id")) {
      try{
        startTest("write-id", "Write the worker's ID", 5);
        runSetup1();
        File f = new File("__worker");

        String id = null;
        if (id == null)
          id = (new Scanner(new File("__worker"+File.separator+"id"))).nextLine();

        if(id == null || id.length() != 5 || id.contains("^a-z"))
            testFailed("The worker ID was not written to __worker/id correctly. It should be a 5-character alphanumeric string, but it was: "+id);

        System.out.println();
        System.out.println("current id: " + id);

        testSucceeded();
      } catch(Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }


    boolean stressRan = false;
    long reqPerSec = 0;
    long numFail = 0;
    if (tests.contains("stress")) try {
      setTimeoutMillis(20000);
      startTest("stress", "Stress test with 5,000 requests", 5);
      final ConcurrentHashMap<String,Integer> ver = new ConcurrentHashMap<String,Integer>();
      final ConcurrentHashMap<String,Integer> lck = new ConcurrentHashMap<String,Integer>();
      final ConcurrentHashMap<String,String> dat = new ConcurrentHashMap<String,String>();
      final int numThreads = 30;
      final int testsTotal = 5000;
      final Counter testsRemaining = new Counter(testsTotal);
      final Counter testsFailed = new Counter(0);
      final String tabname = randomAlphaNum(10,20);
      Thread t[] = new Thread[numThreads];
      long tbegin = System.currentTimeMillis();
      for (int i=0; i<numThreads; i++) {
        t[i] = new Thread("Client thread "+i) { public void run() {
          while (testsRemaining.aboveZero()) {
            try {
              Socket s = openSocket(8001);
              PrintWriter out = new PrintWriter(s.getOutputStream());
              while (testsRemaining.decrement()) {
                String row = randomAlphaNum(1,1);
                if (random(0,1) == 0) {
                  int expectedAtLeastVersion = 0;
                  synchronized(ver) {
                    if (ver.containsKey(row))
                      expectedAtLeastVersion = ver.get(row).intValue();
                  }
                  out.print("GET /data/"+tabname+"/"+row+"/col HTTP/1.1\r\nHost: localhost\r\n\r\n");
                  out.flush();
                  Response r = readAndCheckResponse(s, "response");
                  if (expectedAtLeastVersion > 0) {
                    if (r.statusCode != 200) {
                      testsFailed.increment();
                      System.err.println("GET returned code "+r.statusCode+" for row: "+row+", body: "+new String(r.body));
                    } else {
                      String val = new String(r.body);
                      String[] pcs = val.split("-");
                      int v = Integer.valueOf(pcs[0]).intValue();
                      if (v < expectedAtLeastVersion) {
                        testsFailed.increment();
                        System.err.println("GET returned version "+v+" for row "+row+", but that has been overwritten; we expected at least version "+expectedAtLeastVersion);
                      } if (!dat.get(row+"-"+v).equals(val)) {
                        testsFailed.increment();
                        System.err.println("GET returned value "+val+" for row "+row+", but we expected version "+v+" to be "+dat.get(row+"-"+v).equals(val));
                      }
                    }
                  }
                } else {
                  String val;
                  int nextVersion = 0;
                  synchronized(ver) {
                    while (lck.containsKey(row))
                      row = randomAlphaNum(1,1);
                    lck.put(row, Integer.valueOf(1));
                    if (ver.containsKey(row))
                      nextVersion = ver.get(row).intValue() + 1;
                    val = nextVersion+"-"+randomAlphaNum(10,20);
                    dat.put(row+"-"+nextVersion, val);
                  }
                  out.print("PUT /data/"+tabname+"/"+row+"/col HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+val.length()+"\r\n\r\n"+val);
                  out.flush();
                  Response r = readAndCheckResponse(s, "response");
                  if (r.statusCode != 200) {
                    testsFailed.increment();
                    System.err.println("PUT returned code "+r.statusCode+" for row: "+row+", body: "+new String(r.body));
                  }
                  synchronized(ver) {
                    ver.put(row, nextVersion);
                    lck.remove(row);
                  }
                }
              }
              s.close();
            } catch (Exception e) { e.printStackTrace(); }
          }
        } };
        t[i].start();
      }

      try {
        for (int i=0; i<numThreads; i++) 
          t[i].join();
      } catch (InterruptedException ie) {}

      long tend = System.currentTimeMillis();

      stressRan = true;
      numFail = testsFailed.getValue();
      reqPerSec = 1000*testsTotal/(tend-tbegin);

      if (numFail>0)
        testFailed("Looks like "+numFail+" of the "+testsTotal+" requests failed. Check your code for concurrency issues! (This is a very difficult test case, so you may want to leave it until the very end, when your implementation passes all the other tests.)");
      if (reqPerSec<500)
        testFailed("Looks like your solution handled "+reqPerSec+" requests/second; it should be at least 500. Try disabling any debug output?  (This is a very difficult test case, so you may want to leave it until the very end, when your implementation passes all the other tests.)");

      testSucceeded();
    } catch (Exception e) { 
      StringWriter s = new StringWriter(); 
      e.printStackTrace(new PrintWriter(s)); 
      testFailed("An exception occurred: "+e + s.toString(), false); 
    }

    setTimeoutMillis(-1);


    System.out.println("--------------------------------------------------------\n");
    if (numTestsFailed == 0)
      System.out.println("Looks like your solution passed all of the selected tests. Congratulations!");
    else
      System.out.println(numTestsFailed+" test(s) failed.");
    if (stressRan)  {
      if (reqPerSec > 5000)
        System.out.println("\nYour throughput in the stress test was "+reqPerSec+" requests/sec, with "+numFail+" failures");
      else
        System.out.println(
          "\nYour throughput in the stress test was "+reqPerSec+" requests/sec, with "+numFail+" failures. This is fine on Gradescope, where " +
          "computation power is limited, but if you are running this test on your local machine, try getting at least 5,000 requests/sec." +
          "For comparison, our reference implementation gets abour 14,000 requests/sec on a 2021 M1 MacBook Pro."
        );
    }
    cleanup();
    closeOutputFile();
  }

	public static void main(String args[]) throws Exception {

    /* Make a set of enabled tests. If no command-line arguments were specified, run all tests. */

    Set<String> tests = new TreeSet<String>();
    boolean runSetup1 = true, runSetup2 = false, runTests = true, promptUser = true, outputToFile = false, exitUponFailure = true, cleanup = false;

    if ((args.length > 0) && (args[0].equals("auto1") || args[0].equals("auto2"))) {
      runSetup1 = false;
      runSetup2 = false;
      runTests = true;
      outputToFile = true;
      exitUponFailure = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && (args[0].equals("setup1") || args[0].equals("setup2"))) {
      runSetup1 = args[0].equals("setup1");
      runSetup2 = args[0].equals("setup2");
      runTests = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && (args[0].equals("cleanup1") || (args[0].equals("cleanup2")))) {
      runSetup1 = false;
      runSetup2 = false;
      runTests = false;
      promptUser = false;
      cleanup = true;
    } else if ((args.length > 0) && args[0].equals("version")) {
      System.out.println("HW4 worker autograder v1.3 (Sep 19, 2023)");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto1")) {
//      tests.add("read-id");
      tests.add("put");
      tests.add("putget");
      tests.add("overwrite");
      tests.add("stress");
      tests.add("version");
      tests.add("ifcolumn");
      tests.add("not-found");
      tests.add("two-columns");
      tests.add("id");
    } else if ((args.length > 0) && args[0].equals("auto2")) {
      tests.add("newid");
      tests.add("writeid");
    } 

    for (int i=0; i<args.length; i++)
      if (!args[i].equals("all") && !args[i].equals("auto1") && !args[i].equals("auto2") && !args[i].equals("setup1") && !args[i].equals("setup2") && !args[i].equals("cleanup1") && !args[i].equals("cleanup2"))
        tests.add(args[i]);

    HW4SelfWorkerTest t = new HW4SelfWorkerTest();
    t.setExitUponFailure(exitUponFailure);
    if (outputToFile)
      t.outputToFile();
    if (runSetup1)
      t.runSetup1();
    if (runSetup2)
      t.runSetup2();
    if (promptUser)
      t.prompt();
    if (runTests)
      t.runTests(tests);
    if (cleanup)
      t.cleanup();
  }
} 
