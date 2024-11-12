package org.noova.kvs;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.*;
import java.util.*;
import java.io.*;
import java.net.*;

public class HW4CoordinatorTest extends GenericTest {

  private static Set<String> tests;
  private static Thread serverThread;

  @BeforeClass
  public static void setUp() throws Exception {
    tests = new TreeSet<>();
    tests.add("ping");
    tests.add("table");

    serverThread = new Thread(() -> {
      try {
        org.noova.kvs.Coordinator.main(new String[]{"8000"});
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    serverThread.start();
    Thread.sleep(3000);

    System.out.println("Ensure that the Coordinator is running on port 8000.");
  }


  @AfterClass
  public static void tearDown() {
    if (serverThread != null && serverThread.isAlive()) {
      serverThread.interrupt();  // Stop the server thread
    }
    System.out.println("Tests completed.");
  }

  private Response sendGetRequest(String path) throws Exception {
    Socket s = openSocket(8000);
    PrintWriter out = new PrintWriter(s.getOutputStream());
    out.print("GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n");
    out.flush();
    return readAndCheckResponse(s, "response");
  }

  @Test
  public void testPing() throws Exception {
    try {
      startTest("ping", "Register a new worker using /ping", 5);
      Response r = sendGetRequest("/ping?id=abcde&port=1234");
      assertEquals(200, r.statusCode);
      assertEquals("OK", r.body());
      testSucceeded();
    } catch (Exception e) {
      testFailed("An exception occurred: " + e);
    }
  }

  @Test
  public void testTable() throws Exception {
    try {
      startTest("table", "Worker table as HTML", 5);

      Random rand = new Random();
      int num = 4 + rand.nextInt(3);
      List<String> workers = new LinkedList<>();

      for (int i = 0; i < num; i++) {
        String workerID = randomAlphaNum(5, 7);
        int port = 1024 + rand.nextInt(15000);
        sendGetRequest("/ping?id=" + workerID + "&port=" + port);
        workers.add(workerID);
      }

      Response r = sendGetRequest("/");
      assertEquals(200, r.statusCode);
      assertEquals("text/html", r.headers.get("content-type"));
      assertTrue(r.body().toLowerCase().contains("<html>"));
      assertTrue(r.body().toLowerCase().contains("<table"));
      for (String w : workers) {
        assertTrue(r.body().contains(w));
      }

      testSucceeded();
    } catch (Exception e) {
      testFailed("An exception occurred: " + e);
    }
  }
}
