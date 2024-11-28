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
                pages = kvs.scan(PropertyLoader.getProperty("table.crawler"), startKey, endKey);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }

            Iterator<Row> indexes = null;
            try {
                indexes = kvs.scan(PropertyLoader.getProperty("table.index"), null, null);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }

            generateInvertedIndexBatch(kvs, pages, indexes);

            long end = System.currentTimeMillis();

            System.out.println("Time: " + (end - start) + "ms");

        }

        private static void generateInvertedIndexBatch(KVS kvs, Iterator<Row> pages, Iterator<Row> indexes) throws InterruptedException {
            Map<String, StringBuilder> wordMap = new ConcurrentHashMap<>();
            Map<String, StringBuilder> imageMap = new ConcurrentHashMap<>();
            int pageCount = 0;
            List<String> pageDetails = new ArrayList<>();

            while(pages != null && pages.hasNext()) {
                Row page = pages.next();
                pageCount++;
                pageDetails.add(page.key()+"\n");

                String url = page.get(PropertyLoader.getProperty("table.crawler.url"));
                String text = page.get(PropertyLoader.getProperty("table.crawler.text"));
                //String ip = page.get(PropertyLoader.getProperty("table.crawler.ip"));
                String images = page.get(PropertyLoader.getProperty("table.crawler.images"));

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
                        row = kvs.getRow(PropertyLoader.getProperty("table.index"), word);
                    } catch (IOException e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                    if(row == null){
                        row = new Row(word);
                    }
                    if(row.key().isEmpty()){
                        System.out.println("row key " + row.key() );
                    }

                    links = row.get(PropertyLoader.getProperty("table.index.links"));
                    imgs = row.get(PropertyLoader.getProperty("table.index.images"));

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
                row.put(PropertyLoader.getProperty("table.index.links"), links);
                row.put(PropertyLoader.getProperty("table.index.images"), imgs);
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
                    kvs.putRow(PropertyLoader.getProperty("table.index"), row);

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
            // 返回 alt 属性的值
            return matcher.group(1);
        }
        return "";
    }

}

