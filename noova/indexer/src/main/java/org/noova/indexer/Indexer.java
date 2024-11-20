package org.noova.indexer;

import org.noova.flame.FlameContext;
import org.noova.flame.FlamePair;
import org.noova.flame.FlamePairRDD;
import org.noova.flame.FlameRDD;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Indexer implements Serializable {
    private static final Logger log = Logger.getLogger(Indexer.class);
    static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final boolean ENABLE_PORTER_STEMMING = true;
    private static final boolean ENABLE_IP_INDEX = true;
    private static final String DELIMITER = PropertyLoader.getProperty("default.delimiter");
    private static final int PAGE_LIMIT = 5;

    public static void run(FlameContext ctx, String[] args) {


        try {
            FlameRDD rdd = ctx.fromTable(
                    PropertyLoader.getProperty("table.crawler"),
                    row -> {

                        String url = row.get(PropertyLoader.getProperty("table.crawler.url"));
                        String page = row.get(PropertyLoader.getProperty("table.crawler.page"));
                        String ip = row.get(PropertyLoader.getProperty("table.crawler.ip"));

                        if(ENABLE_IP_INDEX){
                            return url + DELIMITER + page + DELIMITER + ip;
                        }
                        return url + DELIMITER + page;
                    }
            ).filter(s -> !s.isEmpty());

            FlamePairRDD data = rdd.mapToPair(s -> {
                String[] parts = s.split(DELIMITER);

                if(parts.length < 2) {
                    log.info("[indexer] No page found");
                    return new FlamePair(parts[0], "");
                }
                String url = parts[0];
                String page = parts[1];
                log.info("[indexer] url " + url);
                return new FlamePair(url, page);
            });

            data.flatMapToPair(pair -> {
                String url = pair._1();
                String page = pair._2();

                log.info("[indexer] Indexing: " + url);

                //List<String> links = Crawler.parsePageLinks(ctx, page, url, null);

                String filteredContent = filterPage(page);
                String[] words = filteredContent.split(" +");

                if(words.length == 0 || words[0].isEmpty()) {
                    log.warn("[indexer] No words found");
                    return Collections.emptyList();
                }

                List<FlamePair> pairs = new ArrayList<>();
                Map<String, WordStats> wordStats = new HashMap<>();

                for (int i = 0; i < words.length; i++) {

                    String word = words[i];
                    if(word.isEmpty()){
                        log.warn("[indexer] Empty word");
                    }
//                    log.info("[indexer] Word: " + word);

                    processWord(word, i, wordStats);

//                    if(!seen.contains(word)) {
//                        seen.add(word);
//                        pairs.add(new FlamePair(word, url));
//                    }

                    // EC3
                    if(ENABLE_PORTER_STEMMING) {
//                        log.info("[indexer] Stemming: " + word);
                        PorterStemmer stemmer = new PorterStemmer();
                        stemmer.add(word.toCharArray(), word.length());
                        stemmer.stem();
                        String stemmedWord = stemmer.toString();

//                        log.info("[indexer] Original: " + word + " Stemmed: " + stemmedWord);

                        if (!stemmedWord.equals(word)) {
                            processWord(stemmedWord, i, wordStats);
                        }
                    }
                }

                List<Map.Entry<String, WordStats>> sorted = new ArrayList<>(wordStats.entrySet());
                sorted.sort((a, b) -> b.getValue().frequency - a.getValue().frequency);

                for(Map.Entry<String, WordStats> entry : sorted) {
                    String word = entry.getKey();
                    WordStats stats = entry.getValue();
                    pairs.add(new FlamePair(word,
                            String.format("%s:%d %d", url, stats.frequency, stats.firstLocation)));
                }

                return pairs;
            }).foldByKey("", (s, t) -> {
                if(s.isEmpty()) {
                    return t;
                }
                if(t.isEmpty()) {
                    return s;
                }
                List<String> entries = new ArrayList<>();
                entries.addAll(Arrays.asList(s.split(",")));
                entries.addAll(Arrays.asList(t.split(",")));

                entries.sort((a, b) -> {
                    int freqA = Integer.parseInt(a.substring(a.indexOf(":") + 1).split(" ")[0]);
                    int freqB = Integer.parseInt(b.substring(b.indexOf(":") + 1).split(" ")[0]);
                    if(freqA != freqB) {
                        return freqB - freqA;
                    }
                    int locA = Integer.parseInt(a.substring(a.indexOf(":") + 1).split(" ")[1]);
                    int locB = Integer.parseInt(b.substring(b.indexOf(":") + 1).split(" ")[1]);
                    return locA - locB;
                });

                return String.join(",", entries);
            }).saveAsTable(INDEX_TABLE);


            // save ip table
            if(ENABLE_IP_INDEX){
                FlamePairRDD ipTable = rdd.mapToPair(s->{
                    String[] parts = s.split(DELIMITER);
                    if(parts.length < 3) {
                        log.info("[indexer] No page found");
                        return new FlamePair(parts[0], "");
                    }
                    String url = parts[0];
                    String ip = parts[2];
                    log.info("[indexer] url " + url);
                    return new FlamePair(url, ip);
                });

                ipTable.foldByKey("", (s, t) -> {
                    if(s.isEmpty()) {
                        return t;
                    }
                    return s + "," + t;
                }).saveAsTable(PropertyLoader.getProperty("table.url"));

            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String filterPage(String page) {
        if(page == null) return "";

        return page.toLowerCase()
                .strip()
                .replaceAll("<[^>]*>", " ")
                .strip()
                .replaceAll("[.,:;!?''\"()\\-\\r\\n\\t]", " ")
                .strip()
                .replaceAll("[^\\p{L}\\s]", " ")
                .strip();
    }

    private static class WordStats {
        int frequency = 0;
        int firstLocation = Integer.MAX_VALUE;
    }

    private static void processWord(String word, int location, Map<String, WordStats> wordStats) {
        WordStats stats = wordStats.computeIfAbsent(word, k -> new WordStats());
        stats.frequency++;
        stats.firstLocation = Math.min(stats.firstLocation, location);
    }
}
