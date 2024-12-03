package org.noova.crawler;

//import opennlp.tools.lemmatizer.DictionaryLemmatizer;
//import opennlp.tools.postag.POSModel;
//import opennlp.tools.postag.POSTaggerME;
//import opennlp.tools.tokenize.Tokenizer;
//import opennlp.tools.tokenize.TokenizerME;
//import opennlp.tools.tokenize.TokenizerModel;
import org.jsoup.Jsoup;
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
//    private static final Pattern CLEAN_TEXT_PATTERN = Pattern.compile("[^a-zA-Z]+");

    private static final boolean ENABLE_UNPARSED_LINKS = false;
//    private static TokenizerModel tokenizerModel;
//    private static POSModel posModel;
//    private static DictionaryLemmatizer lemmatizer;
//    private static final Set<String> stopWords;
//
//    private static final ThreadLocal<POSTaggerME> posTaggerThreadLocal = ThreadLocal.withInitial(() -> new POSTaggerME(posModel));
//    private static final ThreadLocal<TokenizerME> tokenizerThreadLocal = ThreadLocal.withInitial(() -> new TokenizerME(tokenizerModel));
//    private static final ThreadLocal<DictionaryLemmatizer> lemmatizerThreadLocal = ThreadLocal.withInitial(() -> lemmatizer);
//
//    private static final Set<String> INVALID_POS = Set.of(
//            "DT",  // determiner (the, a, an)
//            "IN",  // preposition (in, of, at)
//            "CC",  // conjunction (and, or, but)
//            "UH",  // interjection (oh, wow, ah)
//            "FW",  // Foreign Word
//            "SYM", // Symbol
//            "LS",  // List item marker
//            "PRP", // Personal pronouns
//            "WDT", // Wh-determiner
//            "WP",  // Wh-pronoun
//            "WRB"  // Wh-adverb
//    );
//
//    static {
//        try {
//
//            InputStream tokenStream = BuildProcessedTable.class.getResourceAsStream("/models/en-token.bin");
//            InputStream posStream = BuildProcessedTable.class.getResourceAsStream("/models/en-pos-maxent.bin");
//            InputStream dictStream = BuildProcessedTable.class.getResourceAsStream("/models/en-lemmatizer.dict.txt");
//
//            if (tokenStream == null) {
//                throw new IOException("Tokenizer model not found in resources: /models/en-token.bin");
//            }
//            if (posStream == null) {
//                throw new IOException("POS model not found in resources: /models/en-pos-maxent.bin");
//            }
//            if (dictStream == null) {
//                throw new IOException("can not find：/models/en-lemmatizer.dict.txt");
//            }
//
//
//            tokenizerModel = new TokenizerModel(tokenStream);
//            posModel = new POSModel(posStream);
//            lemmatizer = new DictionaryLemmatizer(dictStream);
//
//
//            stopWords = new HashSet<>();
//            try (InputStream is = BuildProcessedTable.class.getResourceAsStream("/models/stopwords-en.txt")) {
//                if (is == null) {
//                } else {
//                    BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        if (!line.trim().isEmpty()) {
//                            stopWords.add(line.trim().toLowerCase());
//                        }
//                    }
//                }
//            }
//
//
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to initialize OpenNLP models: " + e.getMessage(), e);
//        }
//    }


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
            String text = page.get("text"); // use original crawled text if any

            if(text.isBlank()){ // use new parseVisibleText if pt-crawl 'text' is empty
                Element body = Jsoup.parse(rawPageContent).body();
                body.select("script, style, .popup, .ad, .banner, [role=dialog], footer, nav, aside, .sponsored, " +
                        ".advertisement, iframe, span[data-icid=body-top-marquee], div[class^=ad-]").remove();
                text = parseVisibleText(body);
            }

            text = Parser.processWord(text); // rm non-ascii + normalize spaces

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

//    private static String generateCleanText(String rawText) {
//        if (rawText == null || rawText.isEmpty()) return "";
//        Tokenizer tokenizer = tokenizerThreadLocal.get();
//        POSTaggerME posTagger = posTaggerThreadLocal.get();
//        DictionaryLemmatizer lemmatizer = lemmatizerThreadLocal.get();
//
//        String[] tokens = tokenizer.tokenize(rawText);
//        String[] posTags = posTagger.tag(tokens);
//        String[] lemmas = lemmatizer.lemmatize(tokens, posTags);
//
//        StringBuilder cleanTextBuilder = new StringBuilder();
//        for (int i = 0; i < lemmas.length; i++) {
//            String lemma = lemmas[i].toLowerCase();
//            if (!stopWords.contains(lemma) && isValidWord(lemma, posTags[i])) {
//                cleanTextBuilder.append(lemma).append(" "); // cleaned single word separated by space
//            }
//        }
//        return cleanTextBuilder.toString().trim();
//    }
//
//    public static boolean isValidWord(String word, String pos) {
//        if (word.length() <= 2) {
//            return false;
//        }
//
//        if (!word.matches("^[a-zA-Z]+$")) {
//            return false;
//        }
//
//        if (word.matches(".*(.)\\1{2,}.*")) {
//            return false;
//        }
//
//        if (INVALID_POS.contains(pos)) {
//            return false;
//        }
//
//        return true;
//    }
}
