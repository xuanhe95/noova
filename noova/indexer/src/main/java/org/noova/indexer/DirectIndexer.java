package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.time.LocalDateTime;


public class DirectIndexer {

    private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static class WordStats {
        int frequency = 0;
        int firstLocation = Integer.MAX_VALUE;
    }

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
            indexes = kvs.scan(PropertyLoader.getProperty("table.index"), startKey, endKey);
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }

        generateInvertedIndexBatch(kvs, pages, indexes);

        long end = System.currentTimeMillis();

        System.out.println("Time: " + (end - start) + "ms");

    }

    private static void generateInvertedIndexBatch(KVS kvs, Iterator<Row> pages, Iterator<Row> indexes) throws InterruptedException {
        Map<String, Map<String, WordStats>> wordMap = new ConcurrentHashMap<>();
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

            String[] words = normalizeWord(text);

            for(int i = 0; i < words.length; i++){
                String word = words[i];
                if(word == null || word.isEmpty() || url == null || url.isEmpty()){
                    continue;
                }
                wordMap.putIfAbsent(word, new ConcurrentHashMap<>());
                Map<String, WordStats> urlStatsMap = wordMap.get(word);

                if (!urlStatsMap.containsKey(url)) {
                    WordStats stats = new WordStats();
                    stats.frequency = 1;
                    stats.firstLocation = i;
                    urlStatsMap.put(url, stats);
                } else {
                    WordStats stats = urlStatsMap.get(url);
                    stats.frequency++;
                    stats.firstLocation = Math.min(stats.firstLocation, i);
                }
            }

            Map<String, Set<String>> wordImageMap = parseImages(images);
            for(Map.Entry<String, Set<String>> entry : wordImageMap.entrySet()){
                String word = entry.getKey();
                Set<String> imageSet = entry.getValue();
                StringBuilder builder = imageMap.computeIfAbsent(word, k -> new StringBuilder());
                for(String image : imageSet){
                    if (!builder.toString().contains(image)) {
                        builder.append(image).append(DELIMITER);
                    }
                }
            }
        }

        try (BufferedWriter pageWriter = new BufferedWriter(new FileWriter("pages_processed.txt", true))) {
            for (String detail : pageDetails) {
                pageWriter.write(detail);
            }
        } catch (IOException e) {
            System.err.println("Error writing pages to file: " + e.getMessage());
        }

        System.out.println("Total pages processed: " + pageCount);

        Set<String> mergedWords = new HashSet<>(wordMap.keySet());
        mergedWords.addAll(imageMap.keySet());

        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        for(String word : mergedWords) {
            Map<String, WordStats> urlStatsMap = wordMap.get(word);
            String images = imageMap.get(word) == null ? "" : imageMap.get(word).toString();

            Row row = null;
            try {
                row = kvs.getRow(PropertyLoader.getProperty("table.index"), word);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }
            if (row == null) {
                row = new Row(word);
            }
            if (row.key().isEmpty()) {
                System.out.println("row key " + row.key());
            }

            String imgs = row.get(PropertyLoader.getProperty("table.index.images"));

            if (urlStatsMap != null) {
                StringBuilder linksBuilder = new StringBuilder();
                for (Map.Entry<String, WordStats> entry : urlStatsMap.entrySet()) {
                    if (linksBuilder.length() > 0) {
                        linksBuilder.append(DELIMITER);
                    }
                    String url = entry.getKey();
                    WordStats stats = entry.getValue();
                    linksBuilder.append(url).append(":").append(stats.frequency).append(",").append(stats.firstLocation);
                }

                String existingLinks = row.get(PropertyLoader.getProperty("table.index.links"));
                if (existingLinks != null && !existingLinks.isEmpty()) {
                    linksBuilder.insert(0, existingLinks + DELIMITER);
                }

                row.put(PropertyLoader.getProperty("table.index.links"), linksBuilder.toString());
            }

            if (imgs == null) {
                imgs = images;
            } else {
                imgs = imgs + DELIMITER + images;
            }
            row.put(PropertyLoader.getProperty("table.index.images"), imgs);

            try {

                count++;
                if (count % 500 == 0) {
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
                System.out.println("Error: " + e.getMessage());

            }
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