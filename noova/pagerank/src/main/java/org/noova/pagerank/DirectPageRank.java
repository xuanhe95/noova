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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
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
    private static final boolean ENABLE_ONLY_BIDIRECTIONAL = true;
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
        //buildGraphBatch(kvs, it);

        Map<String, Double> pageRanks = calculatePageRank(kvs, startKey, endKeyExclusive, totalPages);




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

        Map<String, Integer> rankDistribution = new HashMap<>();

        // 输出结果
        pageRanks.forEach((page, rank) -> {

            double roundedRank = Math.floor(rank * 100) / 100.0;
            rankDistribution.merge(String.valueOf(roundedRank), 1, Integer::sum);
            try {
                kvs.put("pt-pgrk", Hasher.hash(page), "rank", String.valueOf(rank).getBytes());
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

    private static void buildGraphBatch(KVS kvs, Iterator<Row> it) throws IOException {
        Map<String, Row> graphRows = new HashMap<>();

        while(it != null && it.hasNext()){
            Row row = it.next();
            String url = row.get(PropertyLoader.getProperty("table.crawler.url"));
            String links = row.get(PropertyLoader.getProperty("table.crawler.links"));
            Set<String> linkSet = efficientParsePageLinks(links);

            if(linkSet.isEmpty()){
                continue;
            }


            // build graph

            String hashedUrl = Hasher.hash(url);

            Row pageRow = graphRows.getOrDefault(hashedUrl, kvs.getRow(GRAPH_TABLE, hashedUrl));
            // create new row if not exist
            if(pageRow == null){
                pageRow = new Row(hashedUrl);
                graphRows.put(hashedUrl, pageRow);
            }

            String pageLinks = pageRow.get(OUTGOING_COLUMN);
            if(pageLinks == null || pageLinks.isEmpty()){
                pageLinks = "";
            }
            pageLinks += pageLinks + LINE_BREAK_DELIMITER + links;
            pageRow.put(OUTGOING_COLUMN, pageLinks.getBytes());


            // build reversed graph
            for(String link : linkSet){
                String hashedLink = Hasher.hash(link);
                Row linkRow = graphRows.getOrDefault(hashedLink, kvs.getRow(GRAPH_TABLE, hashedLink));
                // create new row if not exist
                if(linkRow == null){
                    linkRow = new Row(hashedLink);
                    graphRows.put(hashedLink, linkRow);
                }

                String linkToPages = linkRow.get(INCOMING_COLUMN);
                if(linkToPages == null || linkToPages.isEmpty()){
                    linkToPages = "";
                }
                linkToPages += url + LINE_BREAK_DELIMITER;
                linkRow.put(INCOMING_COLUMN, linkToPages.getBytes());
            }

            //webGraph.put(url, linkSet);
        }

        for(Map.Entry<String, Row> entry : graphRows.entrySet()) {
            Row row = entry.getValue();
            if(row == null){
                continue;
            }
            kvs.putRow(GRAPH_TABLE, row);
        }

    }

    public static Map<String, Double>calculatePageRankParallel(KVS kvs, Map<String, Double> prevPageRanks, String startRow, String endRowExclusive, int totalPages) throws IOException {
        Map<String, Double> pageRanks = new HashMap<>();
        Map<String, Double> sinkPRs = new HashMap<>();

        Iterator<Row> pages = kvs.scan(GRAPH_TABLE, startRow, endRowExclusive);

        while(pages != null && pages.hasNext()){
            Row page = pages.next();
            String hashedUrl = page.key();

            if(ENABLE_ONLY_BIDIRECTIONAL && !prevPageRanks.containsKey(hashedUrl)){
                continue;
            }

            double rankSum = 0.0;
            double sinkPR = 0.0;

            var linkToPages = page.get(INCOMING_COLUMN);
            if(linkToPages == null){

                sinkPR += prevPageRanks.get(hashedUrl);
            } else{
                Set<String> links = efficientParsePageLinks(linkToPages);


                for(String link : links){
                    String hashedLink = Hasher.hash(link);
                    if(ENABLE_ONLY_BIDIRECTIONAL && !prevPageRanks.containsKey(hashedLink)){
                        continue;
                    }
                    double linkPR = prevPageRanks.get(hashedLink);
                    rankSum += linkPR / links.size();
                }


            }

            double sinkContribution = DECAY_RATE * sinkPR / totalPages;
            double randomJump = (1 - DECAY_RATE);
            pageRanks.put(hashedUrl, randomJump + DECAY_RATE * rankSum + sinkContribution);
        }

        return pageRanks;
    }



    private static Map<String, Double> calculatePageRankParallel(Map<String, Set<String>> webGraph, Map<String, Set<String>> reversedWebGraph, Map<String, Double> prevPageRanks){
        Map<String, Double> pageRanks = webGraph.keySet().parallelStream().collect(
                Collectors.toMap(page -> page, page ->{
                    double rankSum = 0.0;
                    double sinkPR = 0.0;

                    var linkToPages = reversedWebGraph.get(page);
                    if(linkToPages != null){
                        for(String link : linkToPages){
                            Set<String> links = webGraph.get(link);
                            rankSum += prevPageRanks.get(link) / links.size();
                        }
                    } else{
                        sinkPR += prevPageRanks.get(page);
                    }

                    double sinkContribution = DECAY_RATE * sinkPR / totalPages;
                    double randomJump = (1 - DECAY_RATE);
                    return randomJump + DECAY_RATE * rankSum + sinkContribution;
                }));
        return pageRanks;
    }

    public static Map<String, Double> calculatePageRankSerial(Map<String, Set<String>> webGraph, Map<String, Set<String>> reversedWebGraph, Map<String, Double> prevPageRanks) {
        Map<String, Double> pageRanks = new HashMap<>();
        // 合并遍历计算 sink 节点的贡献和每个页面的 PageRank
        for (Map.Entry<String, Set<String>> entry : webGraph.entrySet()) {
            String page = entry.getKey();

            double rankSum = 0.0;
            double sinkPR = 0.0;

            // link to page
            Set<String> linkToPages = reversedWebGraph.get(page);
            if (linkToPages == null) {
                System.out.println("linkToPages is null");
                // this is sink
                sinkPR += prevPageRanks.get(page);
            } else {
                for (String link : linkToPages) {
                    Set<String> linksTo = webGraph.get(link);
                    // each link i contributes to the page PR(i)/L(i)
                    rankSum += prevPageRanks.get(link) / linksTo.size();
                }
            }
            //double sinkPRValue = sinkPages.stream().mapToDouble(prevPageRanks::get).sum();

            // 加入 sink 节点的贡献和跳转因子
            double sinkContribution = DECAY_RATE * sinkPR / totalPages;
            // not apply divide N to each page
            double randomJump = (1 - DECAY_RATE);
            pageRanks.put(page, randomJump + DECAY_RATE * rankSum + sinkContribution);
        }
        return pageRanks;
    }

    public static Map<String, Double> calculatePageRank(KVS kvs, String startKey, String endKeyExclusive, int totalPages) throws IOException {
        Map<String, Double> pageRanks = new HashMap<>();
        Map<String, Double> prevPageRanks;

        Iterator<Row> pages = kvs.scan(GRAPH_TABLE, startKey, endKeyExclusive);
        while(pages != null && pages.hasNext()){
            Row page = pages.next();
            Set<String> columns = page.columns();
            if(ENABLE_ONLY_BIDIRECTIONAL && columns.contains(INCOMING_COLUMN) && columns.contains(OUTGOING_COLUMN)){
                pageRanks.put(page.key(), 1.0);
            }
        }

        int iteration = 0;
        boolean converged;

        Set<String> convergedPages = new HashSet<>();

        do {
            prevPageRanks = pageRanks;
            pageRanks = calculatePageRankParallel(kvs, prevPageRanks, startKey, endKeyExclusive, totalPages);

            converged = checkConvergence(pageRanks, prevPageRanks, CONVERGENCE_THRESHOLD, convergedPages);
            iteration++;
            System.out.println("Iteration: " + iteration);

        } while (!converged && iteration < MAX_ITERATIONS);

        System.out.println("Iterations: " + iteration);

        return pageRanks;
    }


    public static Map<String, Double> calculatePageRank(Map<String, Set<String>> webGraph, Map<String, Set<String>> reversedWebGraph, int totalPages) {
        Map<String, Double> pageRanks = new HashMap<>();
        Map<String, Double> prevPageRanks;

        // 初始化 PageRank 值
        for (String page : webGraph.keySet()) {
            pageRanks.put(page, 1.0);
        }

        int iteration = 0;
        boolean converged;

        Set<String> convergedPages = new HashSet<>();

        do {
            prevPageRanks = pageRanks;
            pageRanks = calculatePageRankParallel(webGraph, reversedWebGraph, prevPageRanks);

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
