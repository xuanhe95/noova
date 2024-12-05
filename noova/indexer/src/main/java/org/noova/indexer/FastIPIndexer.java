package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.KVSUrlCache;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.IPLocation;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

public class FastIPIndexer {
    private static final Logger log = Logger.getLogger(FastIPIndexer.class);
    private static final String DEFAULT_DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String PROCESSED_URL = PropertyLoader.getProperty("table.processed.url");
    private static final String IP_TABLE = PropertyLoader.getProperty("table.ip");
    private static final Map<String, Row> IP_MAP = new HashMap<>();
    private static final Map<String, String> IP_ZIP_CACHE = new HashMap<>();

    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    static int pageCount = 0;

    public static void main(String[] args) throws IOException {
        System.out.println("Starting IP indexing...");
        System.out.println("IP Table Name: " + IP_TABLE);

        System.out.println("Loading URL ID cache...");
        KVSUrlCache.loadAllUrlWithId();
        System.out.println("URL ID cache loaded");

        Iterator<Row> pages = KVS_CLIENT.scan(PROCESSED_TABLE, null, null);
        generateInvertedIndexBatch(pages);
        System.out.println("IP done...");
    }

    private static void generateInvertedIndexBatch(Iterator<Row> pages) {
        ForkJoinPool forkJoinPool = new ForkJoinPool(2 * Runtime.getRuntime().availableProcessors());

        forkJoinPool.submit(() -> pages.forEachRemaining(page -> {
            try {
                processPage(page);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        })).join();

        System.out.println("Total pages processed: " + pageCount);
        saveIndexToTable();
    }

    static Row getIPRow(String ip) {
        Row ipRow = IP_MAP.get(ip);
        if (ipRow == null) {
            try {
                ipRow = KVS_CLIENT.getRow(IP_TABLE, ip);
                if (ipRow == null) {
                    ipRow = new Row(ip);
                }
            } catch (IOException e) {
                log.error("Error fetching ZIP row: " + e.getMessage());
                ipRow = new Row(ip);
            }
            IP_MAP.put(ip, ipRow);
        }
        return ipRow;
    }

    static Row getZipRow(String ip) {
        String zipCode = IP_ZIP_CACHE.get(ip);

        if (zipCode == null) {
            zipCode = IPLocation.getZipFromIP(ip);
            if (zipCode == null || zipCode.isEmpty()) {
                log.error("Could not get ZIP code for IP: " + ip);
                return null;
            }
            IP_ZIP_CACHE.put(ip, zipCode);
            System.out.println("Cached new IP-ZIP mapping: " + ip + " -> " + zipCode);
        } else {
            System.out.println("Using cached ZIP code for IP " + ip + ": " + zipCode);
        }

        Row zipRow = IP_MAP.get(zipCode);
        if (zipRow == null) {
            try {
                zipRow = KVS_CLIENT.getRow(IP_TABLE, zipCode);
                if (zipRow == null) {
                    zipRow = new Row(zipCode);  // 用ZIP作为row key
                }
            } catch (IOException e) {
                log.error("Error fetching ZIP row: " + e.getMessage());
                zipRow = new Row(zipCode);
            }
            IP_MAP.put(zipCode, zipRow);
        }
        return zipRow;
    }

    private static void processIPs(String fromUrl, String ip) {
        if (ip == null || ip.isEmpty()) {
            return;
        }

        String urlId;
        try {
            urlId = KVSUrlCache.getUrlId(fromUrl);
            System.out.println("Processing URL: " + fromUrl + " with IP: " + ip + " urlId: " + urlId);
            Row zipRow = getZipRow(ip);
            if (zipRow != null) {
                zipRow.put(urlId, "1");
                System.out.println("Added URL " + fromUrl + " to ZIP " + zipRow.key());
            }
        } catch (IOException e) {
            log.error("Error fetching url id: " + e.getMessage());
        }
//
//        Row ipRow = getIPRow(ip);
//        ipRow.put(urlId, "1");
//        System.out.println("Added URL " + fromUrl + " to IP " + ip);
    }

    private static void processPage(Row page) throws InterruptedException {
        pageCount++;
        String url = page.get(PROCESSED_URL);
        String ip = page.get("ip");
        processIPs(url, ip);
    }

    private static void saveIndexToTable() {
        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        for (String zip : IP_MAP.keySet()) {
            System.out.println("ZIP: " + zip);
            Row row = IP_MAP.get(zip);
            try {
                count++;
                if (count % 500 == 0) {
                    long currentTime = System.nanoTime();
                    double deltaTime = (currentTime - lastTime) / 1_000_000.0;
                    String formattedTime = LocalDateTime.now().format(formatter);
                    System.out.printf("Count: %d, Time: %s, Delta Time: %" +
                                    ".6f ms%n",
                            count, formattedTime, deltaTime);
                    lastTime = currentTime;
                }
                System.out.println("Saving ZIP: " + zip + " with " + row.columns().size() + " URLs");
                KVS_CLIENT.putRow(IP_TABLE, row);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
