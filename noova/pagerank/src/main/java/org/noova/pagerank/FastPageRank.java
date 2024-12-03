package org.noova.pagerank;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class FastPageRank implements Serializable {
    private static final double DECAY_RATE = 0.85;
    private static final double CONVERGENCE_THRESHOLD = 0.01;
    private static final int MAX_ITERATIONS = 1000000;
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    private static final String OUTGOING_GRAPH = PropertyLoader.getProperty("table.outgoing");
    private static final String INCOMING_GRAPH = PropertyLoader.getProperty("table.incoming");
    private static final Map<String, Row> OUTGOING_GRAPH_CACHE = new HashMap<>();
    private static final Map<String, Row> INCOMING_GRAPH_CACHE = new HashMap<>();

    static double convergenceRatioInPercentage = 100.0;

    private static final Logger log = Logger.getLogger(FastPageRank.class);



    public static void main(String[] args) throws Exception {
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

        int totalPages = KVS_CLIENT.count(PROCESSED_TABLE);

        System.out.println("Total pages: " + totalPages);

        Map<String, Double> pageRanks = processPageRank(totalPages);
        Map<String, Integer> rankDistribution = new HashMap<>();


        pageRanks.forEach((page, rank) -> {

            double roundedRank = Math.floor(rank * 100) / 100.0;
            rankDistribution.merge(String.valueOf(roundedRank), 1, Integer::sum);
            try {
                Row row = new Row(page);
                row.put("rank", String.valueOf(rank).getBytes());
                KVS_CLIENT.putRow("pt-pgrk", row);
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

    public static Map<String, Double> iteratePageRank(Map<String, Double> prevPageRanks, int totalPages, int totalSourcePages) throws IOException {
        Map<String, Double> pageRanks = new HashMap<>();

        // traverse all pages
        for(var pageEntity : OUTGOING_GRAPH_CACHE.entrySet()){
            Row page = pageEntity.getValue();
            String hashedUrl = page.key();


            double rankSum = totalSourcePages * DECAY_RATE / totalPages;
            double sinkPR = 0;

            // all pages link to this page, without source page
            Row reversedPage = INCOMING_GRAPH_CACHE.getOrDefault(hashedUrl, null);
            if(reversedPage == null){
                System.out.println("No incoming graph for " + hashedUrl);
                continue;
            }
            var links = reversedPage.columns();

            // each link i contributes to the page PR(i)/L(i)
            for(String hashedLink : links){
                if(!prevPageRanks.containsKey(hashedLink)){
                    continue;
                }
                Row linkRow = OUTGOING_GRAPH_CACHE.getOrDefault(hashedLink, null);
                Set<String> linkToOthers;
                if(linkRow == null){
                    sinkPR += prevPageRanks.get(hashedLink);
                } else{
                    linkToOthers = linkRow.columns();
                    if(linkToOthers.isEmpty()){
                        // apply sink node
                        sinkPR += prevPageRanks.get(hashedLink);
                    } else{
                        // each link i contributes to the page PR(i)/L(i)
                        rankSum += prevPageRanks.get(hashedLink) / linkToOthers.size();
                    }
                }
            }

            // count sink node contribution to this page
            double sinkContribution = DECAY_RATE * sinkPR / totalPages;
            double randomJump = (1 - DECAY_RATE);
            pageRanks.put(hashedUrl, randomJump + DECAY_RATE * rankSum + sinkContribution);
        }

        return pageRanks;
    }


    public static Map<String, Double> processPageRank(int totalPages) throws IOException {

        Map<String, Double> pageRanks = new HashMap<>();
        Map<String, Double> prevPageRanks;

        // this part should scan all data to make sure all pages (vertices) are included
        Iterator<Row> pages = KVS_CLIENT.scan(OUTGOING_GRAPH, null, null);


        int totalSourcePages = 0;

        while(pages != null && pages.hasNext()){
            Row page = pages.next();
            OUTGOING_GRAPH_CACHE.put(page.key(), page);
            Row reversedPage = KVS_CLIENT.getRow(INCOMING_GRAPH, page.key());
            if(reversedPage == null){
                totalSourcePages++;
                continue;
            }


            INCOMING_GRAPH_CACHE.put(page.key(), reversedPage);




            Set<String> linksFrom = reversedPage.columns();

            if(linksFrom.isEmpty()){
                // source node
                totalSourcePages++;
            } else{
                // normal node and sink node
                pageRanks.put(page.key(), 1.0);
            }
        }

        int iteration = 0;
        boolean converged;

        Set<String> convergedPages = new HashSet<>();

        do {
            prevPageRanks = pageRanks;
            pageRanks = iteratePageRank(prevPageRanks, totalPages, totalSourcePages);

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
