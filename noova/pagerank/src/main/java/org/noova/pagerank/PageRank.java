package org.noova.pagerank;

import org.noova.flame.*;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;
import org.noova.tools.URLParser;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank implements Serializable {
    private static final Logger log = Logger.getLogger(PageRank.class);
    private static final double DECAY_RATE = 0.85;
    private static final double WEIGHTED_FACTOR = 0.15;

    private static final String PAGE_RANK_TABLE = PropertyLoader.getProperty("table.pagerank");

    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");

    private static final String URL_PAGE_DELIMITER = "___";

    private static final String URL_DELIMITER = " ";

    private static final String DUMMY_URL = "Dummy";

    private static double convergenceThreshold = 0.001;
    static double convergenceRatioInPercentage = 100.0;
    public static void run(FlameContext ctx, String[] args) {


        try {
            ctx.getKVS().delete(PAGE_RANK_TABLE);
            ctx.getKVS().delete("sink");
            ctx.getKVS().delete("linkset");
            convergenceThreshold = args.length>0?Double.parseDouble(args[0]):0.01; // use default convergence threshold if not specified

            if(args.length > 1){
                convergenceRatioInPercentage = Double.parseDouble(args[1]);
                if(convergenceRatioInPercentage < 0 || convergenceRatioInPercentage > 100){
                    convergenceRatioInPercentage = 100;
                    log.error("Convergence ratio must be between 0 and 100. Using default value: 100");
                    //throw new IllegalArgumentException("Convergence threshold must be positive");
                }
            } else{
                convergenceRatioInPercentage = 100;
            }

            log.info("[page rank] Overall Convergence Threshold: " + convergenceRatioInPercentage);


            FlameRDD rdd = ctx.fromTable(CRAWL_TABLE, row -> row.get("url") + URL_PAGE_DELIMITER + row.get("page"));
            FlamePairRDD stateTable = rdd.mapToPair(s -> {
                // log.info("[page rank] Mapping: " + s);
                String[] parts = s.split(URL_PAGE_DELIMITER);

                String normalizedUrl = parts[0];
                String hashedUrl = Hasher.hash(parts[0]);
                String page = parts.length > 1 ? parts[1] : "";
                // extract links from the page
                List<String> links = parsePageLinks(ctx, page, normalizedUrl, null);

                Set<String> linkSet = new HashSet<>(links);



                log.info("[page rank load] Links size: " + links.size());
                StringBuilder state = new StringBuilder();
                state.append("1.0").append(",")
                        .append("1.0").append(",");
                // log.info("[page rank load] Page: " + page);
                log.info("[page rank load] Hashed URL: " + hashedUrl);
                log.info("[page rank load] Normalized URL: " + normalizedUrl);
                if(links.isEmpty()){
                    log.warn("[page rank load] This is a sink node: " + normalizedUrl);
                    ctx.getKVS().put("sink", hashedUrl, "url", normalizedUrl );
                }
                for (String link : linkSet) {
                    log.info("[page rank load] In page link: " + link);
                    Row row = ctx.getKVS().getRow("linkset", normalizedUrl);
                    if(row == null){
                        row = new Row(normalizedUrl);
                    }
                    row.put(Hasher.hash(link), link);
                    ctx.getKVS().putRow("linkset", row);

                    state.append(Hasher.hash(link)).append(URL_DELIMITER);
                }
                state.append(DUMMY_URL);
                //state.deleteCharAt(state.length() - URL_DELIMITER.length());
                log.info("[page rank load] State: " + state.toString());
                return new FlamePair(hashedUrl, state.toString());
            });

            // AtomicInteger count = new AtomicInteger(1);
            log.info("[page rank] Starting iterations");
            stateTable = updateTable(stateTable);
            log.info("Updated table");
            while(!isConverged(stateTable)) {
                // log.info("Iteration: " + count.incrementAndGet());
                stateTable = updateTable(stateTable);
            }
            // log.info("[page rank] Converged after " + count.get() + " iterations");

            stateTable.flatMapToPair(pair -> {
                log.info("[output] Pair: " + pair);
                String hashedUrl = pair._1();
                String state = pair._2();
                String[] parts = state.split(",");
                double rank = Double.parseDouble(parts[0]);
                Row row = new Row(hashedUrl);

                log.info("[output] Current: " + rank + " Previous: " + parts[1]);

                row.put("rank", String.valueOf(rank));
                ctx.getKVS().putRow(PAGE_RANK_TABLE, row);
                return new ArrayList<>();
            });

            log.info("[page rank] Finished");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static FlamePairRDD transferTable(FlamePairRDD stateTable) throws Exception {
        FlamePairRDD rdd = stateTable.flatMapToPair(hashedUrlWithState -> {
            log.info("[transfer] ===========================================");
            String hashedUrl = hashedUrlWithState._1();
            String state = hashedUrlWithState._2();

            String[] parts = state.split(",");
            double rankCurrent = Double.parseDouble(parts[0]);
            double rankPrevious = Double.parseDouble(parts[1]);

            log.info("[transfer] Hashed URL: " + hashedUrl);

            List<FlamePair> pairs = new ArrayList<>();

            pairs.add(new FlamePair(hashedUrl, "0.0"));
            // for the case where there are no links
            String[] pageUrls = parts.length > 2 ? parts[2].split(URL_DELIMITER) : null;
            if(pageUrls == null){
                log.warn("[transfer] No links found, malformed state: " + hashedUrl);
                return pairs;
            }
            // remove the dummy URL
            int size = pageUrls.length-1;
            log.info("[transfer] Size: " + size);
            // this should be different with pageUrls, since it should
            if(size == 0){
                log.warn("[transfer] No links found, size is 0: " + hashedUrl);
                return pairs;
            }

            double decayedValuePerLink = DECAY_RATE * rankCurrent / size;
            log.info("[transfer] Decayed value per page: " + decayedValuePerLink);
            for(String pageUrl : pageUrls) {
                if(pageUrl.equals(DUMMY_URL)){
                    // skip the dummy URL
                    continue;
                }

                log.info("[transfer] From URL: " + hashedUrl + " -> Page URL: " + pageUrl);
                //String hashedPageUrl = Hasher.hash(pageUrl);
//                if(pageUrl.equals(hashedUrl)) {
//                    // if point to itself, then no need to transfer
//                    log.info("[transfer] Pointing to itself: " + pageUrl);
//                    pairs.add(new FlamePair(pageUrl, "0.0"));
//                } else{
                    pairs.add(new FlamePair(pageUrl, String.valueOf(decayedValuePerLink)));
//                }
            }
            return pairs;
        });

//        log.info("collecting transfer table");
//        AtomicInteger count = new AtomicInteger(0);
//        rdd.collect().forEach(pair -> {
//            count.getAndIncrement();
//            log.info("[transfer] Pair: " + pair);
//        });
//        log.info("[transfer] Collected " + count.get() + " rows");

        return rdd.foldByKey("0", (s, t) -> {
            double v1 = Double.parseDouble(s);
            double v2 = Double.parseDouble(t);
            log.info("[decayed sum] Fold: " + v1 + " " + v2);
            log.info("[decayed sum]" + (v1 + v2));
            return String.valueOf(v1 + v2);
        });
    }

    private static FlamePairRDD updateTable(FlamePairRDD stateTable) throws Exception {
        log.info("[page rank] Updating table");
        FlamePairRDD transferTable = transferTable(stateTable);

//        stateTable.collect().forEach(pair -> {
//            log.info("[page rank] State: " + pair);
//        });
//
//        transferTable.collect().forEach(pair -> {
//            log.info("[page rank] Transfer: " + pair);
//        });


        return stateTable.join(transferTable).flatMapToPair(hashedUrlWithState -> {
            log.info("[update] ===========================================");
            String hashedUrl = hashedUrlWithState._1();
            String[] state = hashedUrlWithState._2().split(",");
            log.info("[page rank] Parts: " + hashedUrlWithState._2());

            double decayedValue = Double.parseDouble(state[3]);

//            if(state.length < 4){
//                log.warn("[update] Malformed state: " + hashedUrlWithState._2());
//                decayedValue = Double.parseDouble(state[2]);
//            } else {
//                decayedValue = Double.parseDouble(state[3]);
//                //log.info("[page rank] Parts: " + parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]);
//            }
            // make old current rank as previous rank
            double newRankPrevious = Double.parseDouble(state[0]);
            // add the weighted factor to the new current rank
            double newRankCurrent = decayedValue + (1 - DECAY_RATE);

            String pageUrls = state[2];
            log.info("[update] Decayed Value: " + decayedValue);
            log.info("[update] URL: " + hashedUrl  + " New Rank: " + newRankCurrent + " Prev Rank: " + newRankPrevious);
            FlamePair newHashedUrlWithState = new FlamePair(hashedUrl, newRankCurrent + "," + newRankPrevious + "," + pageUrls);
            return Collections.singletonList(newHashedUrlWithState);
        });
    }

    private static boolean isConverged(FlamePairRDD stateTable) throws Exception {
        log.info("Checking for convergence");

        String maxValues = stateTable.flatMap(hashedUrlWithState -> {
            String[] state = hashedUrlWithState._2().split(",");
            double rankCurrent = Double.parseDouble(state[0]);
            double rankPrevious = Double.parseDouble(state[1]);
            double diff = Math.abs(rankCurrent - rankPrevious);
            log.info("[check convergence] Diff: " + diff);
            return Collections.singletonList(String.valueOf(diff));
        }).fold("", (s, t) -> {
            if(s.isEmpty()){
                return t;
            }
            return s + " " + t;
        });

        String[] maxValuesArray = maxValues.split(" ");
        int totalCount = maxValuesArray.length;
        AtomicInteger convergedCount = new AtomicInteger(0);
        double maxValue = Double.MIN_VALUE;
        for(String value : maxValuesArray){
            log.info("[check convergence] Value: " + value);
            if(Double.parseDouble(value) < convergenceThreshold){
                log.warn("[check convergence] Converged by threshold: " + value + " Threshold: " + convergenceThreshold);
                convergedCount.incrementAndGet();
            }
            maxValue = Math.max(maxValue, Double.parseDouble(value));
        }


        log.info("Max Value: " + maxValue);
        log.info("[check convergence] Converged Threshold: " + totalCount * convergenceRatioInPercentage + " " + convergedCount.get() * 100);
        // return (convergedCount / (double) totalCount) * 100 >= convergenceRatioInPercentage;

        if(totalCount * convergenceRatioInPercentage <= convergedCount.get() * 100){
            log.warn("[check convergence] Converged by threshold: " + "Total: " + totalCount + " Converged: " + convergedCount + " Threshold: " + convergenceRatioInPercentage);
            return true;
        }
        return false;
        // return maxValue < convergenceThreshold;
    }

    private static String filterPage(String page) {
        if(page == null){
            return null;
        }
        String regex = "<[^>]*>";
        return page.replaceAll(regex, "");
    }
    static List<String> parsePageLinks(FlameContext ctx, String page, String normalizedUrl, String blacklistTable) throws IOException {
        List<String> links = new ArrayList<>();
        log.info("[crawler] Parsing links from URL: " + normalizedUrl);
        String hashedUrl = Hasher.hash(normalizedUrl);


        String regex = "<a\\s+[^>]*href=[\"']([^\"']*)[\"'][^>]*>([\\s\\S]*?)</a\\s*>";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(page);


        Map<String, StringBuilder> anchorMap = new HashMap<>();

        while (matcher.find()) {

            String href = matcher.group(1).strip();
            log.info("[crawler] Found link: " + href);
            // for EC
            String text = matcher.group(2).strip();


            String normalizedLink = normalizeURL(href, normalizedUrl);
            if(normalizedLink == null){
                log.warn("[crawler] URL " + href + " is not a valid URL. Skipping.");
                continue;
            }

            // log.info("[crawler] add link: " + normalizedLink);

            // anchorMap.put(normalizedLink, anchorMap.getOrDefault(normalizedLink, new StringBuilder()).append(text).append("<br>"));

//            if(isAccessed(ctx, normalizedLink)){
//                log.info("[crawler] URL " + normalizedLink + " is accessed before. Ignore.");
//                continue;
//            }

            links.add(normalizedLink);
        }

        return links;
    }

    static String normalizeURL(String rawUrl, String baseUrl){
        if(rawUrl.contains("#")){
            rawUrl = rawUrl.substring(0, rawUrl.indexOf("#"));
        }
        if(rawUrl.isEmpty()){
            return null;
        }
        if(rawUrl.startsWith("..")){
            rawUrl = rawUrl.replace("..", "");
            rawUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/")) + rawUrl;
        }

        String[] parts = URLParser.parseURL(rawUrl);

        String protocol;
        String host;
        String port;
        String path;

        if (parts[0] == null && parts[1] == null) {
            protocol = URLParser.parseURL(baseUrl)[0] == null ? "http" : URLParser.parseURL(baseUrl)[0];
            host = URLParser.parseURL(baseUrl)[1];
            port = URLParser.parseURL(baseUrl)[2] == null ? "http".equals(protocol) ? "80" : "443" : URLParser.parseURL(baseUrl)[2];
            path = parts[3];
        } else {
            protocol = parts[0] == null ? "http" : parts[0];
            host = parts[1] == null ? URLParser.parseURL(baseUrl)[1] : parts[1];
            port = parts[2] == null ? "http".equals(protocol) ? "80" : "443" : parts[2];
            path = parts[3];
        }
        return protocol + "://" + host + ":" + port + path;
    }



}
