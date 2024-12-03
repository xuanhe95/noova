package org.noova.indexer;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
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
    private static final String DEFAULT_DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String PROCESSED_URL = PropertyLoader.getProperty("table.processed.url");
    private static final String PROCESSED_IMAGES = PropertyLoader.getProperty("table.processed.images");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String IMAGE_MAPPING_TABLE = PropertyLoader.getProperty("table.image-mapping");
    private static final String IMAGE_TABLE = PropertyLoader.getProperty("table.image");
    private static final Map<String, Row> IMAGE_MAP = new HashMap<>();
    private static final Map<String, String> HASHED_IMAGE_MAP = new HashMap<>();

    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    static int pageCount = 0;

    public static void main(String[] args) throws IOException {
        // load url id to the cache
        System.out.println("Loading URL ID...");
        KVSUrlCache.loadUrlId();
        System.out.println("URL ID loaded");

        Iterator<Row> pages = KVS_CLIENT.scan(PROCESSED_TABLE, null, null);
        generateInvertedIndexBatch(pages);

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
//            for (String detail : pageDetails) {
//                pageWriter.write(detail);
//            }
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

    private static void processImages(String fromUrl, String images){
        var wordImageMap = parseImages(images);

        Map<String, StringBuilder> wordToImages = new HashMap<>();

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
                wordToImages.compute(word, (k, v) -> {

                    String hashedImage = Hasher.hash(image);
                    HASHED_IMAGE_MAP.put(hashedImage, image);

                    if(v == null){
                        return new StringBuilder(hashedImage);
                    }
                    return v.append(DEFAULT_DELIMITER).append(hashedImage);
                });
            }
        }

        for(String word : wordToImages.keySet()){
            Row wordRow = getImageRow(word);
            wordRow.put(urlId, wordToImages.get(word).toString());
        }

    }


    private static void processPage(Row page) throws InterruptedException{
        pageCount++;

        String url = page.get(PROCESSED_URL);
        String images = page.get(PROCESSED_IMAGES);

        processImages(url, images);

    }

    private static void saveIndexToTable () {
        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");




        for(String word : IMAGE_MAP.keySet()) {
            Row row = IMAGE_MAP.get(word);
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
                KVS_CLIENT.putRow(IMAGE_TABLE , row);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }


        for(String hashedImage : HASHED_IMAGE_MAP.keySet()) {
            Row row = new Row(hashedImage);
            row.put(PropertyLoader.getProperty("table.default.value"), HASHED_IMAGE_MAP.get(hashedImage));
            try {
                KVS_CLIENT.putRow(IMAGE_MAPPING_TABLE, row);
            } catch (IOException e) {
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
            String[] words = alt.toLowerCase().split("\\s+"); // skip nlp processing for img alt

            for(String word : words){
                word = Parser.removeAfterFirstPunctuation(word);
                word = Parser.processSingleWord(word);
                word = LemmaLoader.getLemma(word);
                // ignore empty words
                if(word == null || word.isBlank() || StopWordsLoader.isStopWord(word)){
                    continue;
                }
                Set<String> imageSet = wordImageMap.computeIfAbsent(word, k -> new HashSet<>());
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