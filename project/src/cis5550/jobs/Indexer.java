package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indexer {
    private static final Logger log = Logger.getLogger(Indexer.class);
    private static final String TABLE_PREFIX = "pt-";
    static final String INDEX_TABLE = TABLE_PREFIX+"index";

    private static final boolean ENABLE_PORTER_STEMMING = true;


    public static void run(FlameContext ctx, String[] args) {

        try {
            FlameRDD rdd = ctx.fromTable("pt-crawl", row -> row.get("url") + "___" + row.get("page"));
            FlamePairRDD data = rdd.mapToPair(s -> {
                String[] parts = s.split("___");
                if(parts.length < 2) {
                    log.info("[indexer] No page found");
                    return new FlamePair(parts[0], "");
                }
                System.out.println("[indexer] url " + parts[0]);
                log.info("[indexer] url " + parts[0]);
                return new FlamePair(parts[0], parts[1]);
            });


            data.flatMapToPair(pair -> {
                String url = pair._1();
                String page = pair._2();

                System.out.println("[indexer] url pair: " + url);

                log.info("[indexer] Indexing: " + url);


                //List<String> links = Crawler.parsePageLinks(ctx, page, url, null);



                String noHtml = page.replaceAll("<[^>]*>", " ").strip();

                log.info("[indexer] No HTML: " + noHtml);

                String noPunctuation = noHtml.replaceAll("[.,:;!?'’\"()\\-\\r\\n\\t]", " ").strip();

                log.info("[indexer] No Punctuation: " + noPunctuation);

                String lowerCase = noPunctuation.toLowerCase().strip();

                String[] words = lowerCase.split(" +");

                if(words.length == 0 || words[0].isEmpty()) {
                    log.warn("[indexer] No words found");
                    return Collections.emptyList();
                }

                List<FlamePair> pairs = new ArrayList<>();


                //Map<String, Integer> wordCount = new HashMap<>();

                Map<String, List<Integer>> wordLocations = new HashMap<>();


                for (int i = 0; i < words.length; i++) {

                    String word = words[i];
                    if(word.isEmpty()){
                        log.warn("[indexer] Empty word");
                    }
                    log.info("[indexer] Word: " + word);
                    //int count = wordCount.getOrDefault(word, 0);
                    //wordCount.put(word, count + 1);

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

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


}
