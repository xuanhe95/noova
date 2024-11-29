package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;


public class URLDict {
    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String URL_ID_TABLE = "pt-urltoid"; // New table for URL sets
    private static final String ID_URL_TABLE = "pt-idtourl"; // New table for key mappings
    private static final String GLOBAL_COUNTER_TABLE = "pt-count"; // Table for global counter
    private static final String GLOBAL_COUNTER_KEY = "global_counter"; // Key for global counter
    private static final String KVS_HOST = PropertyLoader.getProperty("kvs.host");
    private static final String KVS_PORT = PropertyLoader.getProperty("kvs.port");

    public static void main(String[] args) throws IOException {
        System.out.println("Start creating mapping tables...");
        KVS kvs = new KVSClient(KVS_HOST + ":" + KVS_PORT);

        // Initialize the global counter
        int globalCounter = initializeGlobalCounter(kvs);

        int test_run_round = 1;
        // Loop through key pairs (a b, b c, ..., y z)
        for (char c1 = 'a'; c1 <= 'y'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = String.valueOf(c2);
            test_run_round++;
            if (test_run_round>3){
//                break;
            }
            System.out.println("Processing range: " + startKey + " to " + endKey);

            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(CRAWL_TABLE, startKey, endKey);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            Iterator<Row> mapping = null;
            try {
                mapping = kvs.scan(URL_ID_TABLE, null, null);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            // Process each slice and populate the new tables
            globalCounter = processSlice(kvs, pages, mapping);
            updateGlobalCounter(kvs,globalCounter);
        }

        System.out.println("Processing complete. Final counter value: " + globalCounter);
    }

    private static int initializeGlobalCounter(KVS kvs) throws IOException {
        System.out.println("[initializeGlobalCounter] Initializing global counter...");
        Row row = kvs.getRow(GLOBAL_COUNTER_TABLE, GLOBAL_COUNTER_KEY);

        if (row == null) {
            row = new Row(GLOBAL_COUNTER_KEY);

            // If no counter exists, initialize it to 0
            row.put("value", String.valueOf(0));
            kvs.putRow(GLOBAL_COUNTER_TABLE,row);
            System.out.println("[initializeGlobalCounter] Global counter initialized 0");
        }

        return 0;
    }

    private static void updateGlobalCounter(KVS kvs, int globalCounter) throws IOException {
        System.out.println("[updateGlobalCounter] Updating global counter: " + globalCounter);
        Row row = kvs.getRow(GLOBAL_COUNTER_TABLE, GLOBAL_COUNTER_KEY);

        // update counter
        row.put("value", String.valueOf(globalCounter));
        kvs.putRow(GLOBAL_COUNTER_TABLE,row);
        System.out.println("[updateGlobalCounter] Global counter updated");
    }

    private static int processSlice(KVS kvs, Iterator<Row> pages, Iterator<Row> mapping) {
        System.out.println("[processSlice] Processing rows...");
        Map<String, String> map_table = new HashMap<>();
        int counter = 0;
        while (mapping !=null && mapping.hasNext()) {
            Row mappingRow = mapping.next();
            String key = mappingRow.key();
            String value = mappingRow.get("value");
            System.out.println("Processing key: " + key + " value: " + value);
            map_table.put(key, value);
            counter++;
        }
        System.out.println("[processSlice] Finished create map: " + counter);

        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            String rowKey = page.key();
            String url = page.get("url"); // Assuming URL is stored in a column named "url"

            if (url == null) {
                System.out.println("Skipping row with no URL: " + rowKey);
                continue;
            }

            try {
                if (!map_table.containsKey(rowKey)){
                    //System.out.println("add new key: " + rowKey+" value: "+counter);
                    Row row = new Row(rowKey);
                    row.put("value", String.valueOf(counter));
                    row.put("url",url);
                    kvs.putRow(URL_ID_TABLE,row);
                    row = new Row(String.valueOf(counter));
                    row.put("value", rowKey);
                    row.put("url", url);
                    kvs.putRow(ID_URL_TABLE,row);
                    counter++;
                }

            } catch (Exception e) {
                System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
            }
        }
        System.out.println("[processSlice] Finished processing rows: " + counter);
        return counter;
    }
}
