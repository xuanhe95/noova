package org.noova.indexer;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FastIndexer {

    private static final Logger log = Logger.getLogger(FastIndexer.class);
    private static final boolean ENABLE_PARSE_ENTITY = false;
    private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String INDEX_ENTITY_TABLE = PropertyLoader.getProperty("table.index.entity");
    private static final String INDEX_LINKS = PropertyLoader.getProperty("table.index.links");
    private static final String PROCESSED_URL = PropertyLoader.getProperty("table.processed.url");
    private static final String PROCESSED_IMAGES = PropertyLoader.getProperty("table.processed.images");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String INDEX_IMAGES = PropertyLoader.getProperty("table.index.images");
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url-id.id");
    private static final String INDEX_IMAGES_TO_PAGE = PropertyLoader.getProperty("table.index.img-page");

    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();
    static final Map<String, String> URL_ID_CACHE = new WeakHashMap<>();

    public static void main(String[] args) throws IOException {
        KVS kvs = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
        // load url id to the cache
        System.out.println("Loading URL ID...");
        loadUrlId(kvs);
        System.out.println("URL ID loaded");

        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = String.valueOf(c2);
            if (c1 == 'z') {
                endKey = null;
            }
            System.out.println("Processing range: " + startKey + " to " + endKey);

            long start = System.currentTimeMillis();
            System.out.println("Start indexing");
            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(PROCESSED_TABLE, startKey, endKey);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
                return;
            }

            generateInvertedIndexBatch(pages);

            long end = System.currentTimeMillis();

            System.out.println("Time: " + (end - start) + "ms");
        }
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

    static final Map<String, Row> WORD_MAP = new ConcurrentHashMap<>();

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
//        System.out.println("processing: " + page.key());

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
            urlId = getUrlId(url);
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

            for(int i = 0; i < words.length; i++){
                String word = words[i];
                String lemma = LemmaLoader.getLemma(word);
                if(lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma)){
                    continue;
                }

                if(!wordCountInPage.containsKey(lemma)){
                    Row wordRow = getWordRow(lemma);
                    wordRow.put(urlId, String.valueOf(i));
                    wordCountInPage.put(lemma, 1);
                }
                else {
                    wordCountInPage.compute(lemma, (k, v) -> v == null ? 1 : v + 1);
                }
            }


        wordCountInPage.forEach((word, count) -> {
            Row wordRow = getWordRow(word);
            wordRow.put("frequency", String.valueOf( (float) count / totalWordsCountInPage));
        });
    }

    private static void saveIndexToTable () {
        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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

    private static void loadUrlId(KVS kvs) throws IOException {
        var ids = kvs.scan(URL_ID_TABLE, null, null);
        ids.forEachRemaining(row -> {
            String id = row.get(URL_ID_VALUE);
            if(id == null){
                return;
            }
            URL_ID_CACHE.put(row.key(), id);
        });
    }


    private static String getUrlId(String url) throws IOException {
        // helper to find an url's corresponding urlID

        // use cache
        if(URL_ID_CACHE.containsKey(url)){
            System.out.println("URL_ID_CACHE contains url: " + url);
            return URL_ID_CACHE.get(url);
        }

        // use pt-urltoid
        Row row = KVS_CLIENT.getRow(URL_ID_TABLE, Hasher.hash(url));
        if (row != null) {
            String id = row.get(URL_ID_VALUE);
            URL_ID_CACHE.put(url, id);
            return id;
        }

        // didn't find url id in map
        return null;

    }

}