package org.noova.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;
import org.noova.tools.Parser;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BuildProcessedTable {
    /*
    Use pt-crawl to populate pt-processed,
    For NER: use parseVisibleText to convert table.crawler.page to table.processed.rawText
    For Single word + preview: use OpenNLP to store fully-processed (lemmatize+stop rm) text to table.processed.cleanText
    */

    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String KVS_HOST = PropertyLoader.getProperty("kvs.host");
    private static final String KVS_PORT = PropertyLoader.getProperty("kvs.port");
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");

    private static final String UNPARSED_LINKS_TABLE = PropertyLoader.getProperty("table.unparsed");

    private static final Map<String, Row> UNPARSED_LINKS_MAP = new HashMap<>();
    static int counter=0;

    private static final boolean ENABLE_UNPARSED_LINKS = false;


    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Start processing pt-crawl to pt-processed...");
        KVS kvs = new KVSClient(KVS_HOST + ":" + KVS_PORT);

        ExecutorService executor = Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors());

        // dedup
        Set<String> processedKeys = Collections.synchronizedSet(fetchProcessedKeys(kvs));

        long start = System.nanoTime();
        // Loop through key pairs (a b, b c, ..., y z)
        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = (c1 == 'z') ? null : String.valueOf(c2);

            System.out.println("Processing range: " + startKey + " to " + endKey);

            counter=0; // restart counter for next slice

            executor.submit(() -> processSlice(kvs, kvs.scan(CRAWL_TABLE, startKey, endKey), processedKeys));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("Time taken to store to pt-processed: " + (System.nanoTime() - start) / 1_000_000 + "ms");

        try {
            putUnparsedLinksBatch(kvs);
        } catch (IOException e) {
            System.err.println("Error storing unparsed links: " + e.getMessage());
        }
    }

    private static void putUnparsedLinksBatch(KVS kvs) throws IOException {
        for (Map.Entry<String, Row> entry : UNPARSED_LINKS_MAP.entrySet()) {
            kvs.putRow(UNPARSED_LINKS_TABLE, entry.getValue());
        }
    }

    private static Set<String> fetchProcessedKeys(KVS kvs) throws IOException {
        Set<String> processedKeys = new HashSet<>();
        Iterator<Row> processedPages = kvs.scan(PROCESSED_TABLE, null, null);
        while (processedPages != null && processedPages.hasNext()) {
            processedKeys.add(processedPages.next().key());
        }
        return processedKeys;
    }


    private static int processSlice(KVS kvs, Iterator<Row> pages, Set<String> processedKeys) throws IOException, InterruptedException {
        // read crawl pages and store to pt-processed

        // batch put
//        List<Row> buffer = new ArrayList<>();
//        int batchSize = 50;

        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            processPage(page, processedKeys, kvs);
        }

//        if (!buffer.isEmpty()) {
//            batchPutRows(kvs, buffer);
//        }

        System.out.println("[processSlice] Finished processing rows: " + counter);
        return counter;
    }

    private static void processPage(Row page, Set<String> processedKeys, KVS kvs){
        String rowKey = page.key();

        if (processedKeys.contains(rowKey)) {
            System.out.println("Skipping already processed row: " + rowKey);
            return;
        }

        String rawPageContent = page.get("page");

        if (rawPageContent == null || rawPageContent.isEmpty()) {
            System.out.println("Skipping row with no page content: " + rowKey);
            return;
        }

        try {

            // use 'page' in pt-crawl to populate 'rawText' in pt-processed
//            String text = page.get("text"); // use original crawled text if any
//            String text="";
//            if(text.isBlank()){ // use new parseVisibleText if pt-crawl 'text' is empty
//                Element body = Jsoup.parse(rawPageContent).body();
//                body.select("script, style, .popup, .ad, .banner, [role=dialog], footer, nav, aside, .sponsored, " +
//                        ".advertisement, iframe, span[data-icid=body-top-marquee], div[class^=ad-]").remove();
//                text = parseVisibleText(body);
//            }

            Element body = Jsoup.parse(rawPageContent).body();
            body.select("script, style, .popup, .ad, .banner, [role=dialog], footer, nav, aside, .sponsored, " +
                    ".advertisement, iframe, span[data-icid=body-top-marquee], div[class^=ad-]").remove();
            String text = Parser.processWord(parseVisibleText(body));

            // store row for pt-processed
            Row processedRow = new Row(rowKey);
            processedRow.put("text", text);
            if(page.get("url")!=null) processedRow.put("url", page.get("url"));
            if(page.get("ip")!=null) processedRow.put("ip", page.get("ip"));
            if(page.get("title")!=null) processedRow.put("title", page.get("title"));
            if(page.get("images")!=null) processedRow.put("images", page.get("images"));
            if(page.get("timestamp")!=null) processedRow.put("timestamp", page.get("timestamp"));
            if(page.get("description")!=null) processedRow.put("description", page.get("description"));
            if(page.get("addresses")!=null) processedRow.put("addresses", page.get("addresses"));
            if(page.get("icon")!=null) processedRow.put("icon", page.get("icon"));
            //if(page.get("links")!=null) processedRow.put("links", page.get("links")); //! lots of null - need to fix crawler link extraction logic

            // retrieve links from rawPageContent
            String baseUrl = page.get("url");
            if(baseUrl != null){
                Set<String> links = ParseLinks.parsePageLinks(rawPageContent, baseUrl);
                if(!links.isEmpty()){
                    processedRow.put("links", String.join("\n", links));
                    // check weather link is already in the table

                    if(ENABLE_UNPARSED_LINKS) {

                        for (String link : links) {
                            String hashedLink = Hasher.hash(link);
                            boolean exist = kvs.existsRow(URL_ID_TABLE, hashedLink);
                            if (!exist) {
                                // store new link
                                Row row = new Row(hashedLink);
                                row.put(PropertyLoader.getProperty("table.default.value"), link);
                                UNPARSED_LINKS_MAP.put(link, row);
                            }
                        }
                    }

                }
            }

//            buffer.add(processedRow);
//            if (buffer.size() >= batchSize) {
//                batchPutRows(kvs, buffer);
//                buffer.clear();
//            }

            processedKeys.add(rowKey);
            kvs.putRow(PROCESSED_TABLE, processedRow);
            counter++;
            System.out.println("Processed row: " + rowKey);

        } catch (Exception e) {
            System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
            e.printStackTrace();
        }

    }

//    private static void batchPutRows(KVS kvs, List<Row> rows) throws IOException {
//        for (Row row : rows) {
//            kvs.putRow(PROCESSED_TABLE, row);
//        }
//    }

    private static String parseVisibleText(Element element) {
        element.select("script, style").remove();
        StringBuilder textBuilder = new StringBuilder();

        for (Element child : element.children()) {
            if (child.children().isEmpty()) {
                String text = cleanText(child.text()); // use text() for punc; use ownText() for no punc
                if (!text.isEmpty()) {
                    textBuilder.append(text).append("\n");
                }
            } else {
                textBuilder.append(parseVisibleText(child)).append(" ");
            }
        }

        return textBuilder.toString().trim();
    }

    private static String cleanText(String text) {
        text = text.replaceAll("\\[\\s*\\]", " ")
                .replaceAll("\\s{2,}", " ").trim();
        if (text.length() < 3) {
            return "";
        }
        return text;
    }

}
