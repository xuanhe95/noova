package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;

public class FastPDFIndexer {

    private static final Logger log = Logger.getLogger(FastPDFIndexer.class);
    private static final String DEFAULT_DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String PDF_TABLE = PropertyLoader.getProperty("table.pdf");
    private static final String PDF_INDEX_TABLE = PropertyLoader.getProperty("table.pdf.index");

    private static final Map<String, StringBuilder> WORD_TO_HASHED_URLS = new ConcurrentHashMap<>();
    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws IOException {
        System.out.println("Indexing PDF_TABLE...");
        long time = System.currentTimeMillis();
        var rows = KVS_CLIENT.scan(PDF_TABLE, null, null);
        generateInvertedIndexBatch(rows);
        System.out.println("PDF_TABLE indexed: " + (System.currentTimeMillis() - time) + "ms");
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

    private static void processPage(Row page) throws InterruptedException {
        pageCount++;
        pageDetails.add(page.key() + "\n");

        String url = page.get("url");
        String text = page.get("text");

        // Skip invalid rows
        if (url == null || url.isEmpty() || text == null || text.isEmpty()) {
            System.out.println("Skipping invalid row: " + page.key());
            return;
        }

        String cleanText = Parser.processWord(text);
        String hashedUrl = Hasher.hash(url);
        String[] words = cleanText.split("\\s+");

        for (String word : words) {
            word = Parser.removeAfterFirstPunctuation(word);
            String lemma = LemmaLoader.getLemma(word);

            if (lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma)) {
                continue;
            }

            // Append hashed URL to the word's list in WORD_TO_HASHED_URLS
            WORD_TO_HASHED_URLS.computeIfAbsent(lemma, k -> new StringBuilder())
                    .append(hashedUrl).append(DEFAULT_DELIMITER);
        }
    }

    private static void saveIndexToTable() {
        WORD_TO_HASHED_URLS.forEach((word, hashedUrlsBuilder) -> {
            Row row = new Row(word);
            row.put("value", hashedUrlsBuilder.toString());

            try {
                KVS_CLIENT.putRow(PDF_INDEX_TABLE, row);
            } catch (IOException e) {
                System.err.println("Error saving row: " + word + " -> " + e.getMessage());
            }
        });
    }
}
