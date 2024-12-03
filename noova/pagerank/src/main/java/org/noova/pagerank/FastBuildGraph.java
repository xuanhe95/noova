package org.noova.pagerank;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.KVSUrlCache;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;

public class FastBuildGraph {

    private static final String GRAPH_TABLE = PropertyLoader.getProperty("table.graph");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    private static final String OUTGOING_GRAPH = PropertyLoader.getProperty("table.outgoing");
    private static final String INCOMING_GRAPH = PropertyLoader.getProperty("table.incoming");
    private static final boolean ENABLE_ONLY_CRAWLED_PAGES = true;

    public static void main(String[] args) throws IOException {

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


        System.out.println("Key range: " + startKey + " - " + endKeyExclusive);

        long start = System.currentTimeMillis();
        System.out.println("Start building graph");

        Iterator<Row> pages = KVS_CLIENT.scan(PROCESSED_TABLE, startKey, endKeyExclusive);

        System.out.println("Loading URL ID...");
        KVSUrlCache.loadUrlId();
        System.out.println("URL ID loaded");

        buildGraphBatch(pages);
    }


    private static void buildGraphBatch(Iterator<Row> it) throws IOException {
        Map<String, Row> incomingGraph = new HashMap<>();
        Map<String, Row> outgoingGraph = new HashMap<>();

        Map<String, String> hashToUrl = new HashMap<>();

        while(it != null && it.hasNext()){
            Row row = it.next();

            System.out.println("row: " + row.key());

            String url = row.get(PropertyLoader.getProperty("table.processed.url"));
            String links = row.get(PropertyLoader.getProperty("table.processed.links"));
            Set<String> linkSet = DirectPageRank.efficientParsePageLinks(links);

            if(linkSet.isEmpty()) {
                continue;
            }
            // build graph
            processOutgoingBatch(incomingGraph, url, linkSet);
            processIncomingBatch(outgoingGraph, url, linkSet);
        }

        for(Map.Entry<String, Row> entry : incomingGraph.entrySet()) {
            Row row = entry.getValue();
            if(row == null){
                continue;
            }
            KVS_CLIENT.putRow(INCOMING_GRAPH, row);
        }

        for(Map.Entry<String, Row> entry : outgoingGraph.entrySet()) {
            Row row = entry.getValue();
            if(row == null){
                continue;
            }
            KVS_CLIENT.putRow(OUTGOING_GRAPH, row);
        }

    }

    private static void processOutgoingBatch(Map<String, Row> graphRows, String url, Set<String> linkSet) throws IOException {
        String urlId = KVSUrlCache.getUrlId(url);
        String hashedUrl = Hasher.hash(url);

        if(urlId == null){
            return;
        }

        Row pageRow = graphRows.getOrDefault(urlId, KVS_CLIENT.getRow(OUTGOING_GRAPH, urlId));
        // create new row if not exist
        if(pageRow == null){
            pageRow = new Row(urlId);
            graphRows.put(urlId, pageRow);
        }

        for(String link : linkSet){
            String hashedLink = Hasher.hash(link);

            if (ENABLE_ONLY_CRAWLED_PAGES && !KVSUrlCache.checkUrlId(hashedLink)){
                continue;
            }
            pageRow.put(hashedLink, "1".getBytes());
        }
    }

    private static void processIncomingBatch(Map<String, Row> graphRows, String fromUrl, Set<String> linkSet) throws IOException {
        String hashedFromUrl = Hasher.hash(fromUrl);

        // build reversed graph
        for(String link : linkSet){
            String hashedLink = Hasher.hash(link);

            if (ENABLE_ONLY_CRAWLED_PAGES && !KVSUrlCache.checkUrlId(hashedLink)){
                continue;
            }

            Row linkRow = graphRows.getOrDefault(hashedLink, KVS_CLIENT.getRow(GRAPH_TABLE, hashedLink));

            if(linkRow == null){
                linkRow = new Row(hashedLink);
                graphRows.put(hashedLink, linkRow);
            }

            linkRow.put(hashedFromUrl, "1".getBytes());
        }
    }
}
