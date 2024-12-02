package org.noova.pagerank;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class BuildGraph {

    private static final String OUTGOING_COLUMN = PropertyLoader.getProperty("table.graph.outgoing");

    private static final String GRAPH_TABLE = PropertyLoader.getProperty("table.graph");

    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");

    private static final String INCOMING_COLUMN = PropertyLoader.getProperty("table.graph.incoming");

    private static final String LINE_BREAK_DELIMITER = PropertyLoader.getProperty("delimiter.linebreak");

    private static final KVS KVS = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

    private static final Map<String, StringBuilder> OUTGOING_GRAPH_CACHE = new HashMap<>();

    private static final Map<String, StringBuilder> INCOMING_GRAPH_CACHE = new HashMap<>();

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

        Iterator<Row> it = KVS.scan(PROCESSED_TABLE, startKey, endKeyExclusive);

        buildGraphBatch(KVS, it);
    }


    private static Map<String, String> buildGraphBatch(KVS kvs, Iterator<Row> it) throws IOException {
        Map<String, Row> graphRows = new HashMap<>();

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

            String hashedUrl = Hasher.hash(url);

            hashToUrl.put(hashedUrl, url);

            processOutgoingBatch(row, graphRows, hashedUrl, links);


            processIncomingBatch(row, graphRows, url, hashedUrl, linkSet, hashToUrl);

//            webGraph.put(url, linkSet);
        }

        for(Map.Entry<String, Row> entry : graphRows.entrySet()) {
            Row row = entry.getValue();
            if(row == null){
                continue;
            }
            kvs.putRow(GRAPH_TABLE, row);
        }

        return hashToUrl;
    }

    private static void processOutgoingBatch(Row page, Map<String, Row> graphRows, String hashedUrl, String links) throws IOException {
        Row pageRow = graphRows.getOrDefault(hashedUrl, KVS.getRow(GRAPH_TABLE, hashedUrl));
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
    }

    private static void processIncomingBatch(Row page, Map<String, Row> graphRows, String url, String hashedUrl, Set<String> linkSet, Map<String, String> hashToUrl) throws IOException {
        // build reversed graph
        for(String link : linkSet){
            String hashedLink = Hasher.hash(link);

            hashToUrl.put(hashedLink, link);

            Row linkRow = graphRows.getOrDefault(hashedLink, KVS.getRow(GRAPH_TABLE, hashedLink));
            // create new row if not exist
            if(linkRow == null){
                linkRow = new Row(hashedLink);
                graphRows.put(hashedLink, linkRow);
            }

            // try to hit cache first, if not found, get from KVS
            StringBuilder linkToPages = INCOMING_GRAPH_CACHE.getOrDefault(linkRow.key(), new StringBuilder(linkRow.get(INCOMING_COLUMN)));
            if(linkToPages == null){
                linkToPages = new StringBuilder();
            }
            linkToPages.append(url).append(LINE_BREAK_DELIMITER);
            linkRow.put(INCOMING_COLUMN, linkToPages.toString().getBytes());
        }
    }


}
