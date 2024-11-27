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


        KVS kvs = new KVSClient("localhost:8000");

        // 示例：初始化网页及其链接
        Map<String, Set<String>> webGraph = new HashMap<>();



        System.out.println("start");
        Iterator<Row> it = kvs.scan(PropertyLoader.getProperty("table.crawler"), null, null);

        totalPages = kvs.count(PropertyLoader.getProperty("table.crawler"));

        System.out.println("size: " + kvs.count(PropertyLoader.getProperty("table.crawler")));

        while(it != null && it.hasNext()){
            Row row = it.next();
            String url = row.get(PropertyLoader.getProperty("table.crawler.url"));
            String links = row.get(PropertyLoader.getProperty("table.crawler.links"));
            Set<String> linkSet = efficientParsePageLinks(links);
            webGraph.put(url, linkSet);
        }

        Map<String, Set<String>> reversedWebGraph = new HashMap<>();
        webGraph.forEach((page, links) -> {
            links.forEach(link -> {
                reversedWebGraph.computeIfAbsent(link, k -> new HashSet<>()).add(page);
            });
        });



        // 计算 PageRank
        Map<String, Double> pageRanks = calculatePageRank(webGraph, reversedWebGraph, totalPages);

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
