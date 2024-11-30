package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;


public class URLDict {
    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String SITE_COUNT_TABLE  = PropertyLoader.getProperty("table.site-counter"); // Table for site counts
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id"); // New table for URL sets
    private static final String ID_URL_TABLE = PropertyLoader.getProperty("table.id-url"); // New table for key mappings
    private static final String GLOBAL_COUNTER_TABLE = PropertyLoader.getProperty("table.counter"); // Table for global counter
    private static final String GLOBAL_COUNTER_KEY = PropertyLoader.getProperty("table.counter.global"); // Key for global counter
    private static final String KVS_HOST = PropertyLoader.getProperty("kvs.host");
    private static final String KVS_PORT = PropertyLoader.getProperty("kvs.port");

    public static void main(String[] args) throws IOException {
        System.out.println("Start creating mapping tables...");
        KVS kvs = new KVSClient(KVS_HOST + ":" + KVS_PORT);

        // Initialize the global counter
        int globalCounter = initializeGlobalCounter(kvs);

        int test_run_round = 1;
        // Loop through key pairs (a b, b c, ..., y z)
        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = String.valueOf(c2);
            if (c1 == 'z'){
                endKey = null;
            }
            test_run_round++;
            if (test_run_round<27){
                continue;
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

            Iterator<Row> site_count = null;
            try {
                site_count = kvs.scan(SITE_COUNT_TABLE, null, null);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            // Process each slice and populate the new tables
            globalCounter = processSlice(kvs, pages, mapping, site_count);
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

    private static int processSlice(KVS kvs, Iterator<Row> pages, Iterator<Row> mapping,Iterator<Row> site_count) {
        System.out.println("[processSlice] Processing rows...");
        Map<String, String> map_table = new HashMap<>();
        int counter = 0;
        while (mapping !=null && mapping.hasNext()) {
            Row mappingRow = mapping.next();
            String key = mappingRow.key();
            String value = mappingRow.get("value");
            //System.out.println("Processing key: " + key + " value: " + value);
            map_table.put(key, value);
            counter++;
        }
        System.out.println("[processSlice] Finished create mapping map: " + counter);

        Map<String, Integer> hc_map_table = new HashMap<>();
        while (site_count !=null && site_count.hasNext()) {
            Row site_countRow = site_count.next();
            String key = site_countRow.key();
            String value = site_countRow.get("count");
            hc_map_table.put(key, Integer.parseInt(value));
        }
        System.out.println("[processSlice] Finished create head count map. ");


        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            String rowKey = page.key();
            String url = page.get("url"); // Assuming URL is stored in a column named "url"

            String base_url = extractBaseDomain(url);
            if (hc_map_table.containsKey(base_url)){
                hc_map_table.put(base_url,hc_map_table.get(base_url)+1);
            }else{
                hc_map_table.put(base_url,1);
            }

            if (url == null) {
                System.out.println("Skipping row with no URL: " + rowKey);
                continue;
            }

            try {
                if (!map_table.containsKey(rowKey)){
                    //System.out.println("add new key: " + rowKey+" value: "+counter);
                    Row row = new Row(rowKey);
                    row.put("value", String.valueOf(counter));
                    //row.put("url",url);
                    kvs.putRow(URL_ID_TABLE,row);
                    row = new Row(String.valueOf(counter));
                    row.put("value", rowKey);
                    //row.put("url", url);
                    kvs.putRow(ID_URL_TABLE,row);
                    counter++;
                }

            } catch (Exception e) {
                System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
            }
        }

        updateKvsFromMap(kvs,hc_map_table);

        System.out.println("[processSlice] Finished processing rows: " + counter);
        return counter;
    }

    private static String extractBaseUrl(String url) {
        try {
            // Parse the URL and extract the base (e.g., https://example.com:port/)
            java.net.URL parsedUrl = new java.net.URL(url);
            return parsedUrl.getProtocol() + "://" + parsedUrl.getHost() + ":" + parsedUrl.getPort() + "/";
        } catch (Exception e) {
            System.err.println("Invalid URL format: " + url);
            return null;
        }
    }

    private static String extractBaseDomain(String url) {
        try {
            // Parse the URL
            java.net.URL parsedUrl = new java.net.URL(url);
            String host = parsedUrl.getHost();

            // Split the host into parts (e.g., "dsl.cis.upenn.edu" -> ["dsl", "cis", "upenn", "edu"])
            String[] parts = host.split("\\.");

            // Extract the last two parts (e.g., "upenn.edu")
            if (parts.length >= 2) {
                return parts[parts.length - 2] + "." + parts[parts.length - 1];
            }

            // If host is malformed or too short, return it as-is
            return host;
        } catch (Exception e) {
            System.err.println("Invalid URL format: " + url);
            return null;
        }
    }

    private static void updateKvsFromMap(KVS kvs, Map<String, Integer> hc_map_table) {
        System.out.println("[updateKvsFromMap] Updating KVS with site counts...");
        for (Map.Entry<String, Integer> entry : hc_map_table.entrySet()) {
            String baseUrl = entry.getKey();
            int count = entry.getValue();

            try {
                // Get existing row or initialize
                // Write the updated count back to KVS
                Row updatedRow = new Row(baseUrl);
                updatedRow.put("count", String.valueOf(count));
                kvs.putRow(SITE_COUNT_TABLE, updatedRow);

                //System.out.println("[updateKvsFromMap] Updated " + baseUrl + " count to " + count);
            } catch (Exception e) {
                System.err.println("[updateKvsFromMap] Error updating base URL " + baseUrl + ": " + e.getMessage());
            }
        }
        System.out.println("[updateKvsFromMap] KVS updates complete.");
    }

}
