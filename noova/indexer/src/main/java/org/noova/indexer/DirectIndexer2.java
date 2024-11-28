package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DirectIndexer2 {

        private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");
        private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
        private static final String CRAWL_URL = PropertyLoader.getProperty("table.crawler.url");
        private static final String CRAWL_TEXT = PropertyLoader.getProperty("table.crawler.text");
        private static final String CRAWL_IP = PropertyLoader.getProperty("table.crawler.ip");
        private static final String CRAWL_IMAGES = PropertyLoader.getProperty("table.crawler.images");
        private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
        private static final String INDEX_LINKS = PropertyLoader.getProperty("table.index.links");
        private static final String INDEX_IMAGES = PropertyLoader.getProperty("table.index.images");
        static int pageCount = 0;
        static List<String> pageDetails = new ArrayList<>();

        public static void main(String[] args) throws InterruptedException {
            KVS kvs = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

            String startKey = null;
            String endKey = null;
            if(args.length == 1){
                startKey = args[0];
            } else if(args.length > 1){
                startKey = args[0];
                endKey = args[1];
            } else{
                System.out.println("No key range specified, scan all tables");
            }

            System.out.println("Key range: " + startKey + " - " + endKey);

            long start = System.currentTimeMillis();
            System.out.println("Start indexing");
            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(CRAWL_TABLE, startKey, endKey);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }

            Iterator<Row> indexes = null;
            try {
                indexes = kvs.scan(INDEX_TABLE, null, null);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }

            generateInvertedIndexBatch(kvs, pages, indexes);

            long end = System.currentTimeMillis();

            System.out.println("Time: " + (end - start) + "ms");

        }

        private static void processPage(Row page, Map<String, StringBuilder> wordMap, Map<String, StringBuilder> imageMap) throws InterruptedException{
            pageCount++;
            pageDetails.add(page.key()+"\n");

            String url = page.get(CRAWL_URL);
            String text = page.get(CRAWL_TEXT);
            //String ip = page.get(CRAWL_IP);
            String images = page.get(CRAWL_IMAGES);

            //String[] words = text == null ? new String[0] : text.split(" +");
            //String[] image = images.split(" +");

            String[] words = normalizeWord(text);

            Map<String, Set<String>> wordImageMap = parseImages(images);

//                Arrays.stream(words).parallel().forEach(
//                        word -> {
//                            var builder = wordMap.computeIfAbsent(word, k -> new StringBuilder());
//                            builder.append(url).append(DELIMITER);
//                        }
//                );

            for(String word : words) {
                if(word == null || word.isEmpty() || url == null || url.isEmpty()){
                    continue;
                }
                StringBuilder builder = wordMap.computeIfAbsent(word, k -> new StringBuilder());
                builder.append(url).append(DELIMITER);
            }

            for(Map.Entry<String, Set<String>> entry : wordImageMap.entrySet()){
                String word = entry.getKey();
                Set<String> imageSet = entry.getValue();
                StringBuilder builder = imageMap.computeIfAbsent(word, k -> new StringBuilder());
                for(String image : imageSet){
                    builder.append(image).append(DELIMITER);
                }
            }
        }

        private static void generateInvertedIndexBatch(KVS kvs, Iterator<Row> pages, Iterator<Row> indexes) throws InterruptedException {
            Map<String, StringBuilder> wordMap = new ConcurrentHashMap<>();
            Map<String, StringBuilder> imageMap = new ConcurrentHashMap<>();
//            int pageCount = 0;
//            List<String> pageDetails = new ArrayList<>();

            pages.forEachRemaining(page -> {
                // stream parallel processing
                try {
                    processPage(page, wordMap, imageMap);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            System.out.println("Total pages processed: " + pageCount);


            Set<String> mergedWords = new HashSet<>(wordMap.keySet());
            mergedWords.addAll(imageMap.keySet());

            var count = 1;
            long lastTime = System.nanoTime();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            Map<String, LinkImageEntry> indexedMap = new HashMap<>();

            while (indexes != null && indexes.hasNext()) {
                Row index = indexes.next();
                String key = index.key();
                String indexedLinks = index.get("links");
                String indexedImgs = index.get("images");
                indexedMap.put(key, new LinkImageEntry(indexedLinks, indexedImgs));
            }

            final int[] tempCount = {0}; // Counter to track the number of entries printed
            indexedMap.forEach((key, entry) -> {
                if (tempCount[0] < 5) { // Print only the first 5 entries
                    System.out.println("Key: " + key + ", " + entry);
                    tempCount[0]++;
                }
            });
            ///int check_count = 1;
            for(String word : mergedWords){
//                check_count++;
//                if (check_count>50){
//                    return;
//                }
                String urls = wordMap.get(word) == null ? "" : wordMap.get(word).toString();
                String images = imageMap.get(word) == null ? "" : imageMap.get(word).toString();

                Row row = null;
                String links="";
                String imgs="";
                if (indexedMap.containsKey(word)) {
                    row = new Row(word);
                    LinkImageEntry entry = indexedMap.get(word);
                    links= entry.getLinks();
                    imgs = entry.getImgs();
//                    System.out.println("From indexer: " );
//                    System.out.println("Word: " + word);
//                    System.out.println("Links: " + (links != null ? links : "No links available"));
//                    System.out.println("Images: " + (imgs != null ? imgs : "No images available"));
//                    System.out.println("------------------------");
                }
                else{
                    try {
                        row = kvs.getRow(INDEX_TABLE, word);
                    } catch (IOException e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                    if(row == null){
                        row = new Row(word);
                    }
                    if(row.key().isEmpty()){
                        System.out.println("row key " + row.key() );
                    }

                    links = row.get(INDEX_LINKS);
                    imgs = row.get(INDEX_IMAGES);

                }

                if(links == null){
                    links = urls;
                } else{
                    links = links + DELIMITER + urls;
                }

                if(imgs == null){
                    imgs = images;
                } else{
                    imgs = imgs + DELIMITER + images;
                }

                try {
                //System.out.println("word: " + word + " links: " + links);
                row.put(INDEX_LINKS, links);
                row.put(INDEX_IMAGES, imgs);
                //Thread.sleep(10);

                    count++;
                    if (count%500==0){
                        int remainder = count % 1000;
                        long currentTime = System.nanoTime();
                        double deltaTime = (currentTime - lastTime) / 1_000_000.0;
                        String formattedTime = LocalDateTime.now().format(formatter);
                        System.out.printf("Count: %d, %% 1000: %d, Time: %s, Delta Time: %.6f ms%n",
                                count, remainder, formattedTime, deltaTime);
                        lastTime = currentTime;
                    }
                    kvs.putRow(INDEX_TABLE, row);

                } catch (Exception e) {
                    System.out.println("link: " + links + "row key" + row.key() );
                    System.out.println("Error: " + e.getMessage());

                }

            }

            try (BufferedWriter pageWriter = new BufferedWriter(new FileWriter("pages_processed.txt", true))) {
                for (String detail : pageDetails) {
                    pageWriter.write(detail);
                }
            } catch (IOException e) {
                System.err.println("Error writing pages to file: " + e.getMessage());
            }

        }

    public static String[] normalizeWord(String word) {
        if (word == null) {
            return new String[0];
        }
        // Use regex to replace all non-alphabet characters with an empty string
        return word.toLowerCase().replaceAll("[^a-zA-Z]", " ").split(" ");
    }

    static Map<String, Set<String>> parseImages(String images) {
        if(images == null || images.isEmpty()){
            return new HashMap<>();
        }
        Map<String, Set<String>> wordImageMap = new HashMap<>();
        String[] rawImages = images.split("\n");
        for(String rawImage : rawImages){
            String alt = extractImageAlt(rawImage);
            if(alt == null || alt.isEmpty()){
                continue;
            }

            String[] words = normalizeWord(alt);

            for(String word : words){
                if(word == null || word.isBlank()){
                    continue;
                }
                Set<String> imageSet = wordImageMap.computeIfAbsent(word, k -> new HashSet<>());
                imageSet.add(rawImage);
            }
        }
        return wordImageMap;
    }

    static String extractImageAlt(String html) {
        if(html == null || html.isEmpty()){
            return "";
        }
        String regex = "alt\\s*=\\s*\"([^\"]*)\"";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(html);

        if (matcher.find()) {
            // return alt
            return matcher.group(1);
        }
        return "";
    }

}

