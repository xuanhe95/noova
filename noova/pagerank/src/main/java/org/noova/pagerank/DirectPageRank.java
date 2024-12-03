package org.noova.pagerank;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class DirectPageRank implements Serializable {
    private static final double DECAY_RATE = 0.85;
    private static final double CONVERGENCE_THRESHOLD = 0.01;
    private static final int MAX_ITERATIONS = 1000000;

    private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");

    private static final String LINE_BREAK_DELIMITER = PropertyLoader.getProperty("delimiter.linebreak");

    private static final String OUTGOING_COLUMN = PropertyLoader.getProperty("table.graph.outgoing");

    private static final String GRAPH_TABLE = PropertyLoader.getProperty("table.graph");

    private static final String INCOMING_COLUMN = PropertyLoader.getProperty("table.graph.incoming");
    private static final boolean ENABLE_ONLY_BIDIRECTIONAL = false;

    static double convergenceRatioInPercentage = 100.0;

    private static final Logger log = Logger.getLogger(DirectPageRank.class);


    static int totalPages = 0;


    static Set<String> efficientParsePageLinks(String rawLinks) throws IOException {
        if(rawLinks == null || rawLinks.isEmpty()){
            return new HashSet<>();
        }
        Set<String> links = new HashSet<>(List.of(rawLinks.split("\n")));
        System.out.println("links: " + links.size());
        return links;
    }

    public static void main(String[] args) throws Exception {
        KVS kvs = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));


        // Map<String, Set<String>> webGraph = new HashMap<>();

        String startKey = null;
        String endKeyExclusive = null;

        if(args.length == 1){
            startKey = args[0];
        } else if(args.length > 1){
            startKey = args[0];
            endKeyExclusive = args[1];
        } else{
            System.out.println("No key range specified, scan all tables");
        }



        System.out.println("start");
        Iterator<Row> it = kvs.scan(PropertyLoader.getProperty("table.crawler"), startKey, endKeyExclusive);

        totalPages = kvs.count(PropertyLoader.getProperty("table.crawler"));

        System.out.println("size: " + kvs.count(PropertyLoader.getProperty("table.crawler")));
        System.out.println("Building graph");

        // --- STEP 1, run graph in partition ---
//        Map<String, String> hashToUrl = buildGraphBatch(kvs, it);
        // --- ------------------------------ ---

//        buildGraphBatch(kvs, it);


        Map<String, Double> pageRanks = calculatePageRank(kvs, startKey, endKeyExclusive, totalPages);
//---



//        Map<String, Set<String>> reversedWebGraph = new HashMap<>();
//        webGraph.forEach((page, links) -> {
//            links.forEach(link -> {
//                reversedWebGraph.computeIfAbsent(link, k -> new HashSet<>()).add(page);
//            });
//        });
//
//
//        // 计算 PageRank
//        Map<String, Double> pageRanks = calculatePageRank(webGraph, reversedWebGraph, totalPages);

//---
        Map<String, Integer> rankDistribution = new HashMap<>();

        // 输出结果
        pageRanks.forEach((page, rank) -> {

            double roundedRank = Math.floor(rank * 100) / 100.0;
            rankDistribution.merge(String.valueOf(roundedRank), 1, Integer::sum);
            try {
                Row row = new Row(page);
                row.put("rank", String.valueOf(rank).getBytes());
//                row.put("url", hashToUrl.get(page).getBytes());
                kvs.putRow("pt-pgrk", row);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Page: " + page + ", Rank: " + rank);
        });
        rankDistribution.forEach((roundedRank, count) ->
                System.out.println("Rank: " + roundedRank + ", Count: " + count)
        );
    }

    public static Map<String, Double>calculatePageRankParallel(KVS kvs, Map<String, Double> prevPageRanks, String startRow, String endRowExclusive, int totalPages, int totalSourcePages) throws IOException {
        Map<String, Double> pageRanks = new HashMap<>();

        Iterator<Row> pages = kvs.scan(GRAPH_TABLE, startRow, endRowExclusive);


        // traverse all pages
        while(pages != null && pages.hasNext()){
            Row page = pages.next();
            String hashedUrl = page.key();

            if(ENABLE_ONLY_BIDIRECTIONAL && !prevPageRanks.containsKey(hashedUrl)){
                continue;
            }
            if(!prevPageRanks.containsKey(hashedUrl)){
                System.out.println("link not found: " + hashedUrl);
                continue;
            }

            double rankSum = totalSourcePages * DECAY_RATE;
            double sinkPR = 0;

            // all pages link to this page, without source page
            var linkToPages = page.get(INCOMING_COLUMN);

            Set<String> links = efficientParsePageLinks(linkToPages);

            // each link i contributes to the page PR(i)/L(i)
            for(String link : links){
                String hashedLink = Hasher.hash(link);
                if(ENABLE_ONLY_BIDIRECTIONAL && !prevPageRanks.containsKey(hashedLink)){
                    continue;
                }
                Row linkRow = kvs.getRow(GRAPH_TABLE, hashedLink);
                Set<String> linkToOthers = efficientParsePageLinks(linkRow.get(OUTGOING_COLUMN));
                if(linkToOthers.isEmpty()){
                    // apply sink node
                    System.out.println("This should not happen");
                    sinkPR += prevPageRanks.get(hashedLink);
                } else{
                    // each link i contributes to the page PR(i)/L(i)
                    rankSum += prevPageRanks.get(hashedLink) / linkToOthers.size();
                }
            }
            // count sink node contribution to this page
            double sinkContribution = DECAY_RATE * sinkPR / totalPages;
            double randomJump = (1 - DECAY_RATE);
            pageRanks.put(hashedUrl, randomJump + DECAY_RATE * rankSum + sinkContribution);
        }

        return pageRanks;
    }


    public static Map<String, Double> calculatePageRank(KVS kvs, String startKey, String endKeyExclusive, int totalPages) throws IOException {
        Map<String, Double> pageRanks = new HashMap<>();
        Map<String, Double> prevPageRanks;

        // this part should scan all data to make sure all pages (vertices) are included
        Iterator<Row> pages = kvs.scan(GRAPH_TABLE, null, null);

        int totalSourcePages = 0;

        while(pages != null && pages.hasNext()){
            Row page = pages.next();
            Set<String> columns = page.columns();
            if(ENABLE_ONLY_BIDIRECTIONAL && columns.contains(INCOMING_COLUMN) && columns.contains(OUTGOING_COLUMN)){
                pageRanks.put(page.key(), 1.0);
            }
            else{
                if(columns.contains(INCOMING_COLUMN)){
                    // not source page
                    pageRanks.put(page.key(), 1.0);
                } else{
                    // source page, only contribute to other pages
                    totalSourcePages++;
                }
                //pageRanks.put(page.key(), 1.0);
            }
        }

        int iteration = 0;
        boolean converged;

        Set<String> convergedPages = new HashSet<>();

        do {
            prevPageRanks = pageRanks;
            pageRanks = calculatePageRankParallel(kvs, prevPageRanks, null, null, totalPages, totalSourcePages);

            converged = checkConvergence(pageRanks, prevPageRanks, CONVERGENCE_THRESHOLD, convergedPages);
            iteration++;
            System.out.println("Iteration: " + iteration);

        } while (!converged && iteration < MAX_ITERATIONS);

        System.out.println("Iterations: " + iteration);

        return pageRanks;
    }


    private static boolean checkConvergence(Map<String, Double> current, Map<String, Double> previous, double threshold, Set<String> convergedPages) {
        if((double) convergedPages.size() / current.size() * 100 > convergenceRatioInPercentage){
            System.out.println("Converged by ratio: " + convergedPages.size() + " / " + current.size());
            return true;
        }

        for (String page : current.keySet()) {
            //System.out.println("change rate: " + Math.abs(current.get(page) - previous.get(page)));

            if(convergedPages.contains(page)){
                continue;
            }
            else if (Math.abs(current.get(page) - previous.get(page)) > threshold) {
                return false;
            }
            else{
                convergedPages.add(page);
            }
        }



        System.out.println("Converged");
        return true;
    }
}
