package org.noova.gateway.service;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PDFService implements IService {
    private static PDFService instance;

    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    private static final String PDF_INDEX_TABLE = PropertyLoader.getProperty("table.pdf.index");
    private static final String PDF_TABLE = PropertyLoader.getProperty("table.pdf");
    private static final String DEFAULT_DELIMITER = PropertyLoader.getProperty("delimiter.default");

    private PDFService() {
    }

    public static PDFService getInstance() {
        if (instance == null) {
            instance = new PDFService();
        }
        return instance;
    }

    public Map<String, Map<String, String>> searchByKeywordsIntersection(List<String> keywords, int start, int limit) throws IOException {
        Map<String, Map<String, String>> result = new HashMap<>();
        Set<String> matchingHashedUrls = new HashSet<>();

        // Fetch rows for each keyword
        for (String keyword : keywords) {
            Row row = KVS_CLIENT.getRow(PDF_INDEX_TABLE, keyword);
            if (row == null || row.get("value") == null) {
                continue; // Skip if no results for the keyword
            }

            // Parse the `value` column to extract hashed URLs
            String[] hashedUrls = row.get("value").split(DEFAULT_DELIMITER);

            if (matchingHashedUrls.isEmpty()) {
                matchingHashedUrls.addAll(Arrays.asList(hashedUrls));
            } else {
                // Retain only the common hashed URLs across keywords
                matchingHashedUrls.retainAll(Arrays.asList(hashedUrls));
            }
        }

        if (matchingHashedUrls.isEmpty()) {
            return new HashMap<>();
        }

        // Process matching URLs to retrieve metadata
        int count = 0;
        for (String hashedUrl : matchingHashedUrls) {
            if (count >= limit) {
                break;
            }

            Row pdfRow = KVS_CLIENT.getRow(PDF_TABLE, hashedUrl);
            if (pdfRow == null) {
                continue;
            }

            Map<String, String> pdfDetails = new HashMap<>();
            pdfDetails.put("url", pdfRow.get("url"));
            pdfDetails.put("title", pdfRow.get("title"));
            pdfDetails.put("icon", pdfRow.get("icon"));

            result.put(hashedUrl, pdfDetails);
            count++;
        }

        return result;
    }

    public Map<String, Map<String, String>> searchByKeywordsIntersectionAsync(List<String> keywords, int start, int limit) throws IOException {
        Map<String, Map<String, String>> result = new ConcurrentHashMap<>();
        Set<String> matchingHashedUrls = new HashSet<>();

        // Fetch rows for each keyword
        for (String keyword : keywords) {
            Row row = KVS_CLIENT.getRow(PDF_INDEX_TABLE, keyword);
            if (row == null || row.get("value") == null) {
                continue; // Skip if no results for the keyword
            }

            // Parse the `value` column to extract hashed URLs
            String[] hashedUrls = row.get("value").split(DEFAULT_DELIMITER);

            if (matchingHashedUrls.isEmpty()) {
                matchingHashedUrls.addAll(Arrays.asList(hashedUrls));
            } else {
                // Retain only the common hashed URLs across keywords
                matchingHashedUrls.retainAll(Arrays.asList(hashedUrls));
            }
        }

        if (matchingHashedUrls.isEmpty()) {
            return new HashMap<>();
        }

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            Iterator<String> urlIterator = matchingHashedUrls.iterator();

            for (int i = 0; i < limit && urlIterator.hasNext(); i++) {
                String hashedUrl = urlIterator.next();
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        Row pdfRow = KVS_CLIENT.getRow(PDF_TABLE, hashedUrl);
                        if (pdfRow == null) {
                            return;
                        }

                        Map<String, String> pdfDetails = new HashMap<>();
                        pdfDetails.put("url", pdfRow.get("url"));
                        pdfDetails.put("title", pdfRow.get("title"));
                        pdfDetails.put("icon", pdfRow.get("icon"));

                        result.put(hashedUrl, pdfDetails);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            executor.shutdown();
        }

        return result;
    }
}
