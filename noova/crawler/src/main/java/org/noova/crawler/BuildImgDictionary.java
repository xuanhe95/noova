package org.noova.crawler;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Parser;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BuildImgDictionary {
    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String CRAWL_URL_KEY = PropertyLoader.getProperty("table.crawler.url");
    private static final String CRAWL_IMAGE_KEY = PropertyLoader.getProperty("table.crawler.images");
    private static final String IMG_MAPPING_TABLE = PropertyLoader.getProperty("table.image-mapping"); // New table for URL sets
    private static final String GLOBAL_COUNTER_TABLE = PropertyLoader.getProperty("table.counter"); // Table for global counter
    private static final String IMG_COUNTER_KEY = PropertyLoader.getProperty("table.counter.image"); // Key for global counter
    private static final String KVS_HOST = PropertyLoader.getProperty("kvs.host");
    private static final String KVS_PORT = PropertyLoader.getProperty("kvs.port");
    static final String VALID_WORD = "^[a-zA-Z-]+$";

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

            Iterator<Row> image_mapping = null;
            try {
                image_mapping = kvs.scan(IMG_MAPPING_TABLE, null, null);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            // Process each slice and populate the new tables
            globalCounter = processSlice(kvs, pages, image_mapping);
            updateGlobalCounter(kvs,globalCounter);
        }

        System.out.println("Processing complete. Final counter value: " + globalCounter);
    }

    private static int initializeGlobalCounter(KVS kvs) throws IOException {
        System.out.println("[initializeGlobalCounter] Initializing global img counter...");
        Row row = kvs.getRow(GLOBAL_COUNTER_TABLE, IMG_COUNTER_KEY);

        if (row == null) {
            row = new Row(IMG_COUNTER_KEY);

            // If no counter exists, initialize it to 0
            row.put("value", String.valueOf(0));
            kvs.putRow(GLOBAL_COUNTER_TABLE,row);
            System.out.println("[initializeGlobalCounter] Image counter initialized 0");
        }

        return 0;
    }

    private static void updateGlobalCounter(KVS kvs, int imgCounter) throws IOException {
        System.out.println("[updateGlobalCounter] Updating global counter: " + imgCounter);
        Row row = kvs.getRow(GLOBAL_COUNTER_TABLE, IMG_COUNTER_KEY);

        // update counter
        row.put("value", String.valueOf(imgCounter));
        kvs.putRow(GLOBAL_COUNTER_TABLE,row);
        System.out.println("[updateGlobalCounter] IMG counter updated");
    }

    private static int processSlice(KVS kvs, Iterator<Row> pages, Iterator<Row> mapping) {
        System.out.println("[processSlice] Processing rows...");
        Map<String, String> map_table = new HashMap<>();
        int counter = 0;
        while (mapping !=null && mapping.hasNext()) {
            Row mappingRow = mapping.next();
            String key = mappingRow.key();
            String url = map_table.get("url");
            //System.out.println("Processing key: " + key + " value: " + value);
            map_table.put(key, "");
            counter++;
        }

        System.out.println("[processSlice] Finished loading image mapping map: " + counter);

        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            String rowKey = page.key();
            String url = page.get(CRAWL_URL_KEY); // Assuming URL is stored in a column named "url"
            String images = page.get(CRAWL_IMAGE_KEY);
            //System.out.println("url:" + url);
            if (images == null) {
                //System.out.println("Skipping row with no images: " + rowKey);
                continue;
            }

            if (!images.isEmpty()) {
                String[] rawImages = images.split("\n");
                for(String rawImage : rawImages) {
                    String alt = extractImage(rawImage, "alt");
                    // only save the src of the image
                    String src = extractImage(rawImage, "src");
                    if (src == null || src.isBlank()) {
                        continue;
                    }
                    String imageHash = Hasher.hash(src);
                    try {
                        if (!map_table.containsKey(imageHash)) {
                            //System.out.println("add new key: " + rowKey+" value: "+counter);
                            Row row = new Row(imageHash);
                            String rawText = Parser.processWord(alt);
                            row.put("src", src);
                            row.put("alt", Parser.removeAfterFirstPunctuation(rawText));
                            row.put("from_url", url);
                            kvs.putRow(IMG_MAPPING_TABLE, row);
                            counter++;
                            count++;
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
                    }
                    if (count % 10000 == 0) {
                        int remainder = count % 1000;
                        long currentTime = System.nanoTime();
                        double deltaTime = (currentTime - lastTime) / 1_000_000.0;
                        String formattedTime = LocalDateTime.now().format(formatter);
                        System.out.printf("Count: %d, %% 1000: %d, Time: %s, Delta Time: %.6f ms%n" ,
                                count , remainder , formattedTime , deltaTime);
                        lastTime=currentTime;
                    }
                }
            }
        }

        //updateKvsFromMap(kvs,hc_map_table);

        System.out.println("[processSlice] Finished processing rows: " + counter);
        return counter;
    }

    static String extractImage(String html, String tag) {
        if(html == null || html.isEmpty()){
            return "";
        }
        String regex = tag + "\\s*=\\s*\"([^\"]*)\"";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(html);

        if (matcher.find()) {
            // return alt
            return matcher.group(1);
        }
        return "";
    }

}
