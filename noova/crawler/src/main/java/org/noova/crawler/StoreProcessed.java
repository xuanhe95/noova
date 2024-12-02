package org.noova.crawler;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;


import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StoreProcessed {
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

<<<<<<< Updated upstream
    private static final boolean ENABLE_UNPARSED_LINKS = true;
=======
    private static TokenizerModel tokenizerModel;
    private static POSModel posModel;
    private static DictionaryLemmatizer lemmatizer;
    private static final Set<String> stopWords;

    private static final ThreadLocal<POSTaggerME> posTaggerThreadLocal = ThreadLocal.withInitial(() -> new POSTaggerME(posModel));
    private static final ThreadLocal<TokenizerME> tokenizerThreadLocal = ThreadLocal.withInitial(() -> new TokenizerME(tokenizerModel));
    private static final ThreadLocal<DictionaryLemmatizer> lemmatizerThreadLocal = ThreadLocal.withInitial(() -> lemmatizer);

    private static final Set<String> INVALID_POS = Set.of(
            "DT",  // determiner (the, a, an)
            "IN",  // preposition (in, of, at)
            "CC",  // conjunction (and, or, but)
            "UH",  // interjection (oh, wow, ah)
            "FW",  // Foreign Word
            "SYM", // Symbol
            "LS",  // List item marker
            "PRP", // Personal pronouns
            "WDT", // Wh-determiner
            "WP",  // Wh-pronoun
            "WRB"  // Wh-adverb
    );

>>>>>>> Stashed changes

    static {
        try {

            InputStream tokenStream = StoreProcessed.class.getResourceAsStream("/models/en-token.bin");
            InputStream posStream = StoreProcessed.class.getResourceAsStream("/models/en-pos-maxent.bin");
            InputStream dictStream = StoreProcessed.class.getResourceAsStream("/models/en-lemmatizer.dict.txt");

            if (tokenStream == null) {
                throw new IOException("Tokenizer model not found in resources: /models/en-token.bin");
            }
            if (posStream == null) {
                throw new IOException("POS model not found in resources: /models/en-pos-maxent.bin");
            }
            if (dictStream == null) {
                throw new IOException("can not find：/models/en-lemmatizer.dict.txt");
            }


            tokenizerModel = new TokenizerModel(tokenStream);
            posModel = new POSModel(posStream);
            lemmatizer = new DictionaryLemmatizer(dictStream);


            stopWords = new HashSet<>();
            try (InputStream is = StoreProcessed.class.getResourceAsStream("/models/stopwords-en.txt")) {
                if (is == null) {
                } else {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.trim().isEmpty()) {
                            stopWords.add(line.trim().toLowerCase());
                        }
                    }
                }
            }


        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize OpenNLP models: " + e.getMessage(), e);
        }
    }


    public static void main(String[] args) throws InterruptedException {
        System.out.println("Start processing pt-crawl to pt-processed...");
        KVS kvs = new KVSClient(KVS_HOST + ":" + KVS_PORT);

        ExecutorService executor = Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors());

        long start = System.nanoTime();
        // Loop through key pairs (a b, b c, ..., y z)
        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = (c1 == 'z') ? null : String.valueOf(c2);

            System.out.println("Processing range: " + startKey + " to " + endKey);

            executor.submit(() -> processSlice(kvs, kvs.scan(CRAWL_TABLE, startKey, endKey), kvs.scan(PROCESSED_TABLE, null, null)));

//            Iterator<Row> pages = null;
//            try {
//                pages = kvs.scan(CRAWL_TABLE, startKey, endKey);
//            } catch (IOException e) {
//                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
//                continue;
//            }
//
//            Iterator<Row> processedPages = null;
//            try {
//                processedPages = kvs.scan(PROCESSED_TABLE, null, null);
//            } catch (IOException e) {
//                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
//                continue;
//            }
//
//            // Process each slice and populate the new tables
//            int count = processSlice(kvs, pages, processedPages);
//            System.out.println("Rows converted from pt-crawl to pt-processed: "+count);
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


    private static int processSlice(KVS kvs, Iterator<Row> pages, Iterator<Row> processedPages) {
//        System.out.println("[processSlice] Processing rows...");
        HashSet <String> processed_page_table = new HashSet<>();
        int counter = 0;
//        // dedup
        while (processedPages !=null && processedPages.hasNext()) {
            Row mappingRow = processedPages.next();
            String key = mappingRow.key();
            processed_page_table.add(key);
        }

        // read crawl pages and store to pt-processed
        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            String rowKey = page.key();

            if (processed_page_table.contains(rowKey)) {
                System.out.println("Skipping already processed row: " + rowKey);
                continue;
            }

            String rawPageContent = page.get("page");

            if (rawPageContent == null || rawPageContent.isEmpty()) {
                System.out.println("Skipping row with no page content: " + rowKey);
                continue;
            }

            try {
//                System.out.println("Processing row: " + rowKey);
//                System.out.println("Raw data: " + page.toString());

                // use 'page' in pt-crawl to populate 'rawText' in pt-processed
                Element body = Jsoup.parse(rawPageContent).body();
                body.select("script, style, .popup, .ad, .banner, [role=dialog], footer, nav, aside, .sponsored, " +
                        ".advertisement, iframe, span[data-icid=body-top-marquee], div[class^=ad-]").remove();
                String rawText = parseVisibleText(body);

                // use nlp model to generate fully-cleaned text (lemmatize+stop word rm)
                String cleanText = generateCleanText(rawText);

                // store row for pt-processed
                Row processedRow = new Row(rowKey);
                processedRow.put("rawText", rawText); // for entity index
                processedRow.put("cleanText", cleanText); // for single word index
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

                kvs.putRow(PROCESSED_TABLE, processedRow);
                counter++;
                System.out.println("Processed row: " + rowKey);

            } catch (Exception e) {
                System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("[processSlice] Finished processing rows: " + counter);
        return counter;
    }

    private static String parseVisibleText(Element element) {
        StringBuilder textBuilder = new StringBuilder();

        for (Element child : element.children()) {
            String tagName = child.tagName();
            if (tagName.equals("script") || tagName.equals("style")) {
                continue; // Skip non-visible content
            }
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
        text = text.replaceAll("\\[\\s*\\]", " ");
        text = text.replaceAll("\\s{2,}", " ").trim();
        if (text.length() < 3) {
            return "";
        }
        return text;
    }

    private static String generateCleanText(String rawText) {
        if (rawText == null || rawText.isEmpty()) return "";
        Tokenizer tokenizer = tokenizerThreadLocal.get();
        POSTaggerME posTagger = posTaggerThreadLocal.get();
        DictionaryLemmatizer lemmatizer = lemmatizerThreadLocal.get();

        String[] tokens = tokenizer.tokenize(rawText);
        String[] posTags = posTagger.tag(tokens);
        String[] lemmas = lemmatizer.lemmatize(tokens, posTags);

        StringBuilder cleanTextBuilder = new StringBuilder();
        for (int i = 0; i < lemmas.length; i++) {
            String lemma = lemmas[i].toLowerCase();
            if (!stopWords.contains(lemma) && isValidWord(lemma, posTags[i])) {
                cleanTextBuilder.append(lemma).append(" "); // cleaned single word separated by space
            }
        }
        return cleanTextBuilder.toString().trim();
    }

    public static boolean isValidWord(String word, String pos) {
        if (word.length() <= 2) {
            return false;
        }

        if (!word.matches("^[a-zA-Z]+$")) {
            return false;
        }

        if (word.matches(".*(.)\\1{2,}.*")) {
            return false;
        }

        if (INVALID_POS.contains(pos)) {
            return false;
        }

        return true;
    }
}
