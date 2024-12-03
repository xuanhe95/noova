package org.noova.pagerank;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.KVSUrlCache;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.pool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class TopPages {
    static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");

    private static final String PAGERANK_RANK = PropertyLoader.getProperty("table.pagerank.rank");

    private static final String PAGERANK_TABLE = PropertyLoader.getProperty("table.pagerank");

    private static final String TOP_PAGE_TABLE = PropertyLoader.getProperty("table.tops");

    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");


    private static final int LIMIT = 20;


    //private static final Map<PageRankPair, Row> PAGERANK_CACHE = new TreeMap<>();

    private static final Map<String, Row> PAGERANK_CACHE = new ConcurrentHashMap<>();


    public static void main(String[] args) throws IOException {
        System.out.println("Top 10 pages");
        KVSUrlCache.loadAllUrlWithId();


        long overallStart = System.currentTimeMillis();


        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(20, 30, Long.MAX_VALUE, TimeUnit.SECONDS, workQueue);


        String[] startKeys = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
                "r", "s", "t", "u", "v", "w", "x", "y", "z"};

        String[] endKeys = new String[]{"b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
                "r", "s", "t", "u", "v", "w", "x", "y", "z", null};


        for(int i = 0; i < startKeys.length; i++){
            System.out.println("Processing range: " + startKeys[i] + " to " + endKeys[i]);
            String startKey = startKeys[i];
            String endKey = endKeys[i];
            System.out.println("Processing range: " + startKey + " to " + endKey);

            long start = System.currentTimeMillis();
            System.out.println("Start indexing");

            threadPool.execute(() -> {
                try {
                    processPartition(startKey, endKey);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        while (true) {
            if (threadPool.getActiveCount() == 0) {
                threadPool.shutdown();
                break;
            }
        }






//        for (char c1 = 'a'; c1 <= 'z'; c1++) {
//            char c2 = (char) (c1 + 1); // Next character for endKey
//            String startKey = String.valueOf(c1);
//            String endKey = String.valueOf(c2);
//            if (c1 == 'z') {
//                endKey = null;
//            }
//            System.out.println("Processing range: " + startKey + " to " + endKey);
//
//            long start = System.currentTimeMillis();
//            System.out.println("Start indexing");
//
//
//
//
//            //processPartition(startKey, endKey);
//
//            long end = System.currentTimeMillis();
//
//            System.out.println("Time: " + (end - start) + "ms");
//        }
        long overallEnd = System.currentTimeMillis();
        System.out.println("Overall time: " + (overallEnd - overallStart) + "ms");
    }

    static void processPartition(String startKey, String endKeyExclusive) throws IOException {

        var words = KVS_CLIENT.scan(INDEX_TABLE, startKey, endKeyExclusive);

        while (words != null && words.hasNext()) {
            var wordRow = words.next();
            var topPages = processWord(wordRow);

            Row row = new Row(wordRow.key());

            var pagesSetIter =  topPages.entrySet().iterator();

            for(int i = 0; i < LIMIT && pagesSetIter.hasNext(); i++){
                String page = pagesSetIter.next().getValue();
                row.put(String.valueOf(i), page);
            }

            KVS_CLIENT.putRow(TOP_PAGE_TABLE, row);
        }
    }

    static SortedMap<Double, String> processWord(Row wordRow) throws IOException {
        SortedMap<Double, String> topPages = new TreeMap<>((a,b) -> Double.compare(b, a));
        String word = wordRow.key();

        Set<String> links = wordRow.columns();

        //System.out.println("Processing word: " + word + " with " + links.size() + " links");


        for (var linkId : links) {
            String hashedLink = KVSUrlCache.getHashedUrl(linkId);

            if(hashedLink == null){
                continue;
            }

            var rankRow = PAGERANK_CACHE.get(hashedLink);

            if(rankRow == null) {
                rankRow = KVS_CLIENT.getRow(PAGERANK_TABLE, hashedLink);
                if(rankRow == null){
                    continue;
                }
                PAGERANK_CACHE.put(hashedLink, rankRow);
            }

            String rank = rankRow.get(PAGERANK_RANK);
            if(rank == null){
                continue;
            }
            topPages.put(Double.parseDouble(rank), hashedLink);
        }

        return topPages;
    }

}
