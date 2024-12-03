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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FastImageIndexer {

    private static final Logger log = Logger.getLogger(FastImageIndexer.class);
    private static final boolean ENABLE_PARSE_ENTITY = false;
    private static final String DEFAULT_DELIMITER = PropertyLoader.getProperty("delimiter.default");

    private static final String COMMA_DELIMITER = PropertyLoader.getProperty("delimiter.comma");

    private static final String COLON_DELIMITER = PropertyLoader.getProperty("delimiter.colon");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String INDEX_ENTITY_TABLE = PropertyLoader.getProperty("table.index.entity");
    private static final String INDEX_LINKS = PropertyLoader.getProperty("table.index.links");
    private static final String PROCESSED_URL = PropertyLoader.getProperty("table.processed.url");
    private static final String PROCESSED_IMAGES = PropertyLoader.getProperty("table.processed.images");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String IMAGE_TABLE = PropertyLoader.getProperty("table.image");
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url-id.id");
    private static final String INDEX_IMAGES_TO_PAGE = PropertyLoader.getProperty("table.index.img-page");
    private static final Map<String, Row> WORD_MAP = new ConcurrentHashMap<>();
    private static final Map<String, Row> IMAGE_MAP = new ConcurrentHashMap<>();

    private static final long MAX_SUFFIX_LENGTH = 10 * 1024;
    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    private static final boolean ENABLE_WORD_SUFFIX = true;
    private static final int SUFFIX_LENGTH = 3;
    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();
    static final Map<String, String> URL_ID_CACHE = new WeakHashMap<>();

    public static void main(String[] args) throws IOException {
        // load url id to the cache
        System.out.println("Loading URL ID...");
        KVSUrlCache.loadUrlId();
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
                pages = KVS_CLIENT.scan(PROCESSED_TABLE, startKey, endKey);
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


    static Row getImageRow(String word){
        Row imageRow = IMAGE_MAP.get(word);
        if(imageRow == null){
            try {
                imageRow = KVS_CLIENT.getRow(IMAGE_TABLE, word);
                if(imageRow == null){
                    imageRow = new Row(word);
                }
            } catch (IOException e) {
                log.error("Error fetching image row: " + e.getMessage());
                imageRow = new Row(word);
            }
            IMAGE_MAP.put(word, imageRow);
        }
        return imageRow;

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

    private static void processImages(String fromUrl, String images){
        var wordImageMap = parseImages(images);

        String urlId = null;
        try{
            urlId = KVSUrlCache.getUrlId(fromUrl);
        } catch (IOException e){
            log.error("Error fetching url id: " + e.getMessage());
            return;
        }

        for(String word : wordImageMap.keySet()){
            Row wordRow = getImageRow(word);
            Set<String> imageSet = wordImageMap.get(word);
            for(String image : imageSet){
                //String hashedImage = Hasher.hash(image);
                wordRow.put(urlId, image);
            }
        }

    }

    private static void processWord(String url, String cleanText){
        // skip op-heavy nlp tasks if url DNE
        if (url == null || url.isEmpty()) {
            System.out.println("Skipping row with no URL: " + url);
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
            System.out.println("Skipping row with no url id mapped: " + url);
            return;
        }


        if(cleanText == null || cleanText.isEmpty()){
            System.out.println("Skipping row with no clean text: " + url);
            return;
        }


        String[] words = cleanText.split("\\s+");
        int totalWordsCountInPage = words.length;
        Map<String, Integer> wordCountInPage = new HashMap<>();
        // populate wordMap for single index using cleanText from pt-processed

        Map<String, StringBuilder> suffixMap = new HashMap<>();
        for(int i = 0; i < words.length; i++){
            String word = words[i].toLowerCase();
            //word = Parser.processWord(word);
            word = Parser.removeAfterFirstPunctuation(word);
            String lemma = LemmaLoader.getLemma(word);
            //String lemma = LemmaLoader.getLemma(word);
            if(lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma)){
                continue;
            }

            if(!wordCountInPage.containsKey(lemma)){
                //Row wordRow = getWordRow(lemma);

                if(ENABLE_WORD_SUFFIX){
                    StringBuilder builder = suffixMap.computeIfAbsent(lemma, k -> new StringBuilder());

                    if(builder.length() > MAX_SUFFIX_LENGTH){
                        continue;
                    }
                    // word position in page

                    // automatically append to buffer
                    // position,word1,word2.. e.g. 1,word1,word2,word3,

                    builder.append(i);
                    for(int j = 1; j <= SUFFIX_LENGTH; j++){
                        int pos = i+j;
                        if(pos < words.length){
                            builder.append(COMMA_DELIMITER).append(words[pos]);
                        }
                    }
                    builder.append(DEFAULT_DELIMITER);
                }
                wordCountInPage.put(lemma, 1);
            }
            else {
                wordCountInPage.compute(lemma, (k, v) -> v == null ? 1 : v + 1);
            }
        }


        wordCountInPage.forEach((word, count) -> {
            Row wordRow = getWordRow(word);

            // final result:
            // 0,word1,word2,word3 /n 1,word1,word2,word3,word4 /n :0.4


            StringBuilder builder = suffixMap.getOrDefault(word, new StringBuilder());
            builder.append(COLON_DELIMITER).append((double) count / totalWordsCountInPage);

            wordRow.put(urlId, builder.toString());
        });
    }

    private static void processPage(Row page) throws InterruptedException{
        pageCount++;
        pageDetails.add(page.key()+"\n");
//        System.out.println("processing: " + page.key());

        String url = page.get(PROCESSED_URL);
        String cleanText = page.get("text"); // cleanText for single-word parsing
        String images = page.get(PROCESSED_IMAGES);

        processImages(url, images);
        processWord(url, cleanText);


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

    static Map<String, Set<String>> parseImages(String images) {
        if(images == null || images.isEmpty()){
            return new HashMap<>();
        }
        Map<String, Set<String>> wordImageMap = new HashMap<>();
        String[] rawImages = images.split("\n");
        for(String rawImage : rawImages){
            String alt = extractImage(rawImage, "alt");
            // only save the src of the image
            String src = extractImage(rawImage, "src");
            if(alt == null || alt.isBlank() || src == null || src.isBlank()){
                continue;
            }

//            String[] words = tokenizeText(alt);
            String[] words = alt.split("\\s+"); // skip nlp processing for img alt

            for(String word : words){
                // ignore empty words
                if(word == null || word.isBlank()){
                    continue;
                }
                //word = Parser.processWord(word).toLowerCase();
                word = Parser.removeAfterFirstPunctuation(word);

                String lemma = LemmaLoader.getLemma(word);
                System.out.println("Word: " + word+ " lemma: " + lemma);
                if (lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma)) {
                    continue;
                }

                Set<String> imageSet = wordImageMap.computeIfAbsent(lemma, k -> new HashSet<>());
                imageSet.add(src);
            }
        }
        return wordImageMap;
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