package org.noova.pagerank;

import org.noova.flame.*;
import org.noova.kvs.Coordinator;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;
import org.noova.tools.URLParser;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.noova.pagerank.PageRank.*;

public class DirectPageRank implements Serializable {
    private static final double DECAY_RATE = 0.85;
    private static final double CONVERGENCE_THRESHOLD = 0.01;
    private static final int MAX_ITERATIONS = 1000000;

    private static final Logger log = Logger.getLogger(DirectPageRank.class);


    static List<String> efficientParsePageLinks(String rawLinks) throws IOException {
        if(rawLinks == null || rawLinks.isEmpty()){
            return new ArrayList<>();
        }
        List<String> links = new ArrayList<>(List.of(rawLinks.split("\n")));
        System.out.println("links: " + links.size());
        return links;
    }

    public static void main(String[] args) throws Exception {


        KVS kvs = new KVSClient("localhost:8000");

        // 示例：初始化网页及其链接
        Map<String, List<String>> webGraph = new HashMap<>();

        System.out.println("start");
        Iterator<Row> it = kvs.scan(PropertyLoader.getProperty("table.crawler"), null, null);

        System.out.println("size: " + kvs.count(PropertyLoader.getProperty("table.crawler")));

        while(it != null && it.hasNext()){
            Row row = it.next();
            String url = row.get(PropertyLoader.getProperty("table.crawler.url"));
            String links = row.get(PropertyLoader.getProperty("table.crawler.links"));
            List<String> linkList = efficientParsePageLinks(links);
            webGraph.put(url, linkList);
        }
        // 计算 PageRank
        Map<String, Double> pageRanks = calculatePageRank(webGraph);




        // 输出结果
        pageRanks.forEach((page, rank) -> {

            try {
                kvs.put("pt-pgrk", page, "rank", String.valueOf(rank).getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Page: " + page + ", Rank: " + rank);
        });
    }
    public static Map<String, Double> calculatePageRank(Map<String, List<String>> webGraph) {
        // 初始化 PageRank 值
        int totalPages = webGraph.size();
        Map<String, Double> pageRanks = new HashMap<>();
        Map<String, Double> prevPageRanks = new HashMap<>();

        for (String page : webGraph.keySet()) {
            pageRanks.put(page, 1.0 / totalPages);
        }

        int iteration = 0;
        boolean converged;

        do {
            prevPageRanks.putAll(pageRanks);
            Map<String, Double> tempRanks = new HashMap<>();
            double sinkPR = 0.0;

            // 计算 sink 节点的总 PageRank 值
            for (String page : webGraph.keySet()) {
                if (webGraph.get(page).isEmpty()) { // 如果页面没有出链
                    sinkPR += prevPageRanks.get(page);
                }
            }

            // 计算每个页面的新 PageRank 值
            for (String page : webGraph.keySet()) {
                double rankSum = 0.0;

                // 遍历所有页面，检查是否有链接到当前页面
                for (Map.Entry<String, List<String>> entry : webGraph.entrySet()) {
                    String linkingPage = entry.getKey();
                    List<String> links = entry.getValue();

                    if (links.contains(page)) {
                        rankSum += prevPageRanks.get(linkingPage) / links.size();
                    }
                }

                // 加入 sink 节点的贡献和跳转因子
                double sinkContribution = DECAY_RATE * sinkPR / totalPages;
                double randomJump = (1 - DECAY_RATE) / totalPages;

                tempRanks.put(page, randomJump + DECAY_RATE * rankSum + sinkContribution);
            }

            // 更新 PageRank
            pageRanks.putAll(tempRanks);

            // 检查收敛性
            converged = checkConvergence(pageRanks, prevPageRanks, CONVERGENCE_THRESHOLD);
            if (iteration == 0) {
                converged = false;
            }
            iteration++;
            System.out.println("Iteration: " + iteration);

        } while (!converged && iteration < MAX_ITERATIONS);

        System.out.println("Iterations: " + iteration);

        return pageRanks;
    }

    private static boolean checkConvergence(Map<String, Double> current, Map<String, Double> previous, double threshold) {
        for (String page : current.keySet()) {
            //System.out.println("change rate: " + Math.abs(current.get(page) - previous.get(page)));
            if (Math.abs(current.get(page) - previous.get(page)) > threshold) {
                return false;
            }
        }
        System.out.println("Converged");
        return true;
    }
}
