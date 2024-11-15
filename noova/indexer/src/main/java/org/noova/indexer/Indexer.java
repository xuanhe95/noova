package org.noova.indexer;

import org.noova.flame.FlameContext;
import org.noova.flame.FlamePair;
import org.noova.flame.FlamePairRDD;
import org.noova.flame.FlameRDD;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.Serializable;
import java.util.*;

public class Indexer implements Serializable {
    private static final Logger log = Logger.getLogger(Indexer.class);
    static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final boolean ENABLE_PORTER_STEMMING = true;
    private static final boolean ENABLE_IP_INDEX = true;
    private static final String DELIMITER = PropertyLoader.getProperty("default.delimiter");




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
            );
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

                Map<String, List<Integer>> wordLocations = new HashMap<>();

                for (int i = 0; i < words.length; i++) {

                    String word = words[i];
                    if(word.isEmpty()){
                        log.warn("[indexer] Empty word");
                    }
                    log.info("[indexer] Word: " + word);

                    List<Integer> locations = wordLocations.getOrDefault(word, new ArrayList<>());
                    locations.add(i);
                    wordLocations.put(word, locations);

//                    if(!seen.contains(word)) {
//                        seen.add(word);
//                        pairs.add(new FlamePair(word, url));
//                    }

                    // EC3
                    if(ENABLE_PORTER_STEMMING) {
                        log.info("[indexer] Stemming: " + word);
                        PorterStemmer stemmer = new PorterStemmer();
                        stemmer.add(word.toCharArray(), word.length());
                        stemmer.stem();
                        String stemmedWord = stemmer.toString();

                        log.info("[indexer] Original: " + word + " Stemmed: " + stemmedWord);
                        if (stemmedWord.equals(word)) {
                            continue;
                        }

                        List<Integer> stemmedLocations = wordLocations.getOrDefault(stemmedWord, new ArrayList<>());
                        stemmedLocations.add(i);
                        wordLocations.put(stemmedWord, stemmedLocations);
                    }
                }

                List<Map.Entry<String, List<Integer>>> sorted = new ArrayList<>(wordLocations.entrySet());
                sorted.sort((a, b) -> b.getValue().size() - a.getValue().size());

                for(Map.Entry<String, List<Integer>> entry : sorted) {
                    log.info("Word: " + entry.getKey() + " Count: " + entry.getValue().size());
                    String word = entry.getKey();
                    List<Integer> locations = entry.getValue();
                    StringBuilder builder = new StringBuilder();
                    for(int i : locations) {
                        builder.append(i).append(" ");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    pairs.add(new FlamePair(word, url + ":" + builder));
                }

                return pairs;
            }).foldByKey("", (s, t) -> {
                if(s.isEmpty()) {
                    return t;
                }
                List<String> leftSide = new ArrayList<>(List.of(s.split(",")));
                List<String> rightSide = Arrays.asList(t.split(","));

                log.info("[indexer] Left: " + leftSide);
                log.info("[indexer] Right: " + rightSide);

                leftSide.addAll(rightSide);

                leftSide.sort((a, b) -> {
                    String[] aParts = a.substring(a.indexOf(":") + 1).split(" ");
                    String[] bParts = b.substring(b.indexOf(":") + 1).split(" ");
                    if(aParts.length != bParts.length) {
                        return bParts.length - aParts.length;
                    } else {
                        return aParts[0].compareTo(bParts[0]);
                    }
                });

                return String.join(",", leftSide);
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
        if(page == null) {
            return "";
        }

        String filtedText = page.toLowerCase().strip();

        filtedText = filtedText.replaceAll("<[^>]*>", " ").strip();

        log.info("[indexer] No HTML: " + filtedText);

        filtedText = filtedText.replaceAll("[.,:;!?'’\"()\\-\\r\\n\\t]", " ").strip();

        log.info("[indexer] No Punctuation: " + filtedText);

        // filter out non-letters
        filtedText = filtedText.replaceAll("[^\\p{L}\\s]", " ").strip();

        return filtedText;
    }


}
