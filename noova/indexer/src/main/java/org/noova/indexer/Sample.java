package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Sample {
    private static final String CRAWL_TABLE = "pt-crawl";
     private static final String KVS_HOST = "localhost";
    private static final String KVS_PORT = "8000";
    private static final String sample_table = "pt-sample";


    public static void main(String[] args) throws IOException {
        System.out.println("Start creating mapping tables...");
        KVS kvs = new KVSClient(KVS_HOST + ":" + KVS_PORT);

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
                //continue;
            }
            System.out.println("Processing range: " + startKey + " to " + endKey);

            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(CRAWL_TABLE, startKey, endKey);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            // Process each slice and populate the new tables
            processSlice(kvs, pages);
        }

        System.out.println("Processing complete.");
    }

    private static void processSlice(KVS kvs, Iterator<Row> pages) {
        System.out.println("[processSlice] Processing rows...");
        Map<String, Integer> map_count_table = new HashMap<>();
        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            String rowKey = page.key().substring(0, 3);

            try {
                if (map_count_table.containsKey(rowKey)){
                    int rkcount = map_count_table.get(rowKey);
                    if (rkcount<3){
                        kvs.putRow(sample_table,page);
                    }
                    map_count_table.put(rowKey,rkcount+1);
                }else{
                    kvs.putRow(sample_table,page);
                    map_count_table.put(rowKey,1);
                }

            } catch (Exception e) {
                System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
            }
        }

    }
}
