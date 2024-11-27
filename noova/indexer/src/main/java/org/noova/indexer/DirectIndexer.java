package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DirectIndexer {

        private static final String DELIMITER = "|||";
        private static final boolean ENABLE_PORTER_STEMMING = true;
        private static final boolean ENABLE_IP_INDEX = true;

        public static void main(String[] args) throws InterruptedException {
            KVS kvs = new KVSClient("localhost:8000");

            long start = System.currentTimeMillis();

            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(PropertyLoader.getProperty("table.crawler"), null, null);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }

            generateInvertedIndexBatch(kvs, pages);

            long end = System.currentTimeMillis();

            System.out.println("Time: " + (end - start) + "ms");

        }

        private static void generateInvertedIndexBatch(KVS kvs, Iterator<Row> pages) throws InterruptedException {
            Map<String, StringBuilder> wordMap = new ConcurrentHashMap<>();

            while(pages != null && pages.hasNext()) {
                Row page = pages.next();
                String url = page.get(PropertyLoader.getProperty("table.crawler.url"));
                String text = page.get(PropertyLoader.getProperty("table.crawler.text"));
                //String ip = page.get(PropertyLoader.getProperty("table.crawler.ip"));
                //String images = page.get(PropertyLoader.getProperty("table.crawler.images"));

                //String[] words = text == null ? new String[0] : text.split(" +");
                //String[] image = images.split(" +");

                String[] words = normalizeWord(text);

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
            }

            for(Map.Entry<String, StringBuilder> entry : wordMap.entrySet()){
                String word = entry.getKey();
                String urls = entry.getValue().toString();

                Row row = null;
                try {
                    row = kvs.getRow(PropertyLoader.getProperty("table.index"), word);
                } catch (IOException e) {
                    System.out.println("Error: " + e.getMessage());
                }
                if(row == null){
                    row = new Row(word);
                }
                String links = row.get(PropertyLoader.getProperty("table.index.links"));
                if(links == null){
                    links = urls;
                } else{
                    links = links + DELIMITER + urls;
                }
                //System.out.println("word: " + word + " links: " + links);
                row.put(PropertyLoader.getProperty("table.index.links"), links);
                //Thread.sleep(10);
                try {
                    kvs.putRow(PropertyLoader.getProperty("table.index"), row);
                } catch (IOException e) {
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
}
