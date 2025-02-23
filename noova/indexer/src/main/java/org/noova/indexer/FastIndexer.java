package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.KVSUrlCache;
import org.noova.kvs.Row;
import org.noova.tools.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;


public class FastIndexer {

    private static final Logger log = Logger.getLogger(FastIndexer.class);
    private static final String DEFAULT_DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String COMMA_DELIMITER = PropertyLoader.getProperty("delimiter.comma");
    private static final String COLON_DELIMITER = PropertyLoader.getProperty("delimiter.colon");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String PROCESSED_URL = PropertyLoader.getProperty("table.processed.url");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final Map<String, Row> WORD_MAP = new HashMap<>();
    private static final Map<String, Map<String, StringBuilder>> URL_WORD_MAP = new HashMap<>();
    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    private static final int SUFFIX_LENGTH = 0;
    private static final int PARSE_POSITION_LIMIT = 20;
    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();
    static final Map<String, String> URL_ID_CACHE = new WeakHashMap<>();

    public static void main(String[] args) throws IOException {
        // load url id to the cache
        System.out.println("Loading URL ID...");
        KVSUrlCache.loadAllUrlWithId();
        System.out.println("URL ID loaded");

        var rows = KVS_CLIENT.scan(PROCESSED_TABLE, null, null);
        generateInvertedIndexBatch(rows);

//        for (char c1 = 'a'; c1 <= 'z'; c1++) {
//            char c2 = (char) (c1 + 1); // Next character for endKey
//            String startKey = String.valueOf(c1);
//            String endKey = String.valueOf(c2);
//            if (c1 == 'z') {
//                endKey = null;
//            }
//            System.out.println("Processing range: " + startKey + " to " + endKey);
//
//            long start = System.currentTimeMillis();
//            System.out.println("Start indexing");
//            Iterator<Row> pages = null;
//            try {
//                pages = KVS_CLIENT.scan(PROCESSED_TABLE, startKey, endKey);
//            } catch (IOException e) {
//                System.out.println("Error: " + e.getMessage());
//                return;
//            }
//
//            generateInvertedIndexBatch(pages);
//
//            long end = System.currentTimeMillis();
//
//            System.out.println("Time: " + (end - start) + "ms");
//        }
    }

    private static void generateInvertedIndexBatch(Iterator<Row> pages) {

        ForkJoinPool forkJoinPool = new ForkJoinPool(2*Runtime.getRuntime().availableProcessors());

        forkJoinPool.submit(() -> pages.forEachRemaining(page -> {
            try {
                processPage(page);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        })).join();


        try (BufferedWriter pageWriter = new BufferedWriter(new FileWriter("pages_processed.txt", true))) {
            for (String detail : pageDetails) {
                pageWriter.write(detail);
            }
        } catch (IOException e) {
            System.err.println("Error writing pages to file: " + e.getMessage());
        }

        System.out.println("Total pages processed: " + pageCount);

        saveIndexToTable();
    }


    static Row getWordRow(String word) {

        Row wordRow = WORD_MAP.get(word);
        if(wordRow == null){
            try {
                wordRow = KVS_CLIENT.getRow(INDEX_TABLE, word);
                if(wordRow == null){
                    wordRow = new Row(word);
                }
            } catch (IOException e) {
                log.error("Error fetching word row: " + e.getMessage());
                wordRow = new Row(word);
            }
            WORD_MAP.put(word, wordRow);
        }

        return wordRow;
    }

    private static void processPage(Row page) throws InterruptedException{
        pageCount++;
        pageDetails.add(page.key()+"\n");

        String url = page.get(PROCESSED_URL);
        String cleanText = page.get("text"); // cleanText for single-word parsing

        // skip op-heavy nlp tasks if url DNE
        if (url == null || url.isEmpty()) {
            System.out.println("Skipping row with no URL: " + page.key());
            return;
        }

        // get urlId base on url
        String urlId;
        try {
            urlId = KVSUrlCache.getUrlId(url);
        } catch (IOException e) {
            System.err.println("Error fetching/assigning ID for URL: " + url + ", " + e.getMessage());
            return;
        }

        // URL_ID map does not have url mapped
        if (urlId==null) {
            System.out.println("Skipping row with no url id mapped: " + page.key());
            return;
        }


        if(cleanText == null || cleanText.isEmpty()){
            System.out.println("Skipping row with no clean text: " + page.key());
            return;
        }


        String[] words = cleanText.split("\\s+");
        int totalWordsCountInPage = words.length;
        Map<String, Integer> wordCountInPage = new HashMap<>();
        // populate wordMap for single index using cleanText from pt-processed



        Map<String, StringBuilder> suffixMap = new HashMap<>();
        for(int i = 0; i < words.length; i++){
            String word = words[i];
            //word = Parser.processWord(word);
            word = Parser.removeAfterFirstPunctuation(word);
            String lemma = LemmaLoader.getLemma(word);
            //String lemma = LemmaLoader.getLemma(word);
//                if(lemma == null || lemma.isEmpty()){
//                    continue;
//                }
            if(lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma)){
                continue;
            }


            wordCountInPage.compute(lemma, (k, v) -> v == null ? 1 : v + 1);
            if(wordCountInPage.getOrDefault(lemma, 0) < PARSE_POSITION_LIMIT){
                //Row wordRow = getWordRow(lemma);


                StringBuilder builder = suffixMap.computeIfAbsent(lemma, k -> new StringBuilder());

//                        if(builder.length() > MAX_SUFFIX_LENGTH){
//                            continue;
//                        }
                // word position in page

                // automatically append to buffer
                // position,word1,word2.. e.g. 1,word1,word2,word3,
                    // append location
                    builder.append(i);
                    // append suffix words
                    for(int j = 1; j <= SUFFIX_LENGTH; j++){
                        int pos = i+j;
                        if(pos < words.length){
                            String nextWord = Parser.processSingleWord(words[pos]);
                            nextWord = LemmaLoader.getLemma(nextWord);
                            if(nextWord == null || nextWord.isEmpty() || StopWordsLoader.isStopWord(lemma)){
                                continue;
                            }
                            builder.append(COMMA_DELIMITER).append(nextWord);
                        }
                    }
                    builder.append(DEFAULT_DELIMITER);
            }
        }



        // max freq for tf
        int maxFrequency = wordCountInPage.values().stream().max(Integer::compare).orElse(1);
//        System.out.println("maxFrequency: "+maxFrequency);

        wordCountInPage.forEach((word, count) -> {
            // Row wordRow = getWordRow(word);

            // final result:
            // 0,word1,word2,word3 /n 1,word1,word2,word3,word4 /n :0.4

            // !!latest ver: position:normalized doctf
//            StringBuilder builder = suffixMap.get(word);
//            if(builder!=null){
//                double docTF = 0.4+(1-0.4) * (double) count/maxFrequency; // precompute doc tf
//                builder.append(COLON_DELIMITER).append(docTF).append(DEFAULT_DELIMITER);
//            }
            StringBuilder builder = suffixMap.getOrDefault(word, new StringBuilder());
            double docTF = 0.4+(1-0.4) * (double) count/maxFrequency; // precompute doc tf
            builder.append(COLON_DELIMITER).append(docTF).append(DEFAULT_DELIMITER);
//            StringBuilder builder = suffixMap.getOrDefault(word, new StringBuilder());
//            builder.append(COLON_DELIMITER).append(count).append("/").append(totalWordsCountInPage).append(DEFAULT_DELIMITER);
            //wordRow.put(urlId, builder.toString());
        });

        URL_WORD_MAP.put(url, suffixMap);
    }

    private static void saveIndexToTable () {
        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



        for(String url : URL_WORD_MAP.keySet()) {
            Map<String, StringBuilder> suffixMap = URL_WORD_MAP.get(url);
            for(String word : suffixMap.keySet()) {
                Row row = getWordRow(word);
                StringBuilder builder = suffixMap.get(word);
                String urlId = null;
                try {
                    urlId = KVSUrlCache.getUrlId(url);
                } catch (IOException e) {
                    System.err.println("Error fetching/assigning ID for URL: " + url + ", " + e.getMessage());
                    continue;
                }

                row.put(urlId, builder.toString());
            }
        }





        for(String word : WORD_MAP.keySet()) {
            Row row = WORD_MAP.get(word);
            try {

                count++;
                if (count % 500 == 0) {
                    int remainder = count % 1000;
                    long currentTime = System.nanoTime();
                    double deltaTime = (currentTime - lastTime) / 1_000_000.0;
                    String formattedTime = LocalDateTime.now().format(formatter);
                    System.out.printf("Count: %d, %% 1000: %d, Time: %s, Delta Time: %.6f ms%n" ,
                            count , remainder , formattedTime , deltaTime);
                    lastTime=currentTime;
                }

                KVS_CLIENT.putRow(INDEX_TABLE , row);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }


}