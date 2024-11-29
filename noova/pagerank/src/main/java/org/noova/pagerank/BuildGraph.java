package org.noova.pagerank;

import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class BuildGraph {

    private static final String OUTGOING_COLUMN = PropertyLoader.getProperty("table.graph.outgoing");

    private static final String GRAPH_TABLE = PropertyLoader.getProperty("table.graph");

    private static final String INCOMING_COLUMN = PropertyLoader.getProperty("table.graph.incoming");

    private static final String LINE_BREAK_DELIMITER = PropertyLoader.getProperty("delimiter.linebreak");

    private static final KVS KVS = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

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

        Iterator<Row> it = KVS.scan(GRAPH_TABLE, startKey, endKeyExclusive);

        buildGraphBatch(KVS, it);
    }


    private static Map<String, String> buildGraphBatch(KVS kvs, Iterator<Row> it) throws IOException {
        Map<String, Row> graphRows = new HashMap<>();

        Map<String, String> hashToUrl = new HashMap<>();

        while(it != null && it.hasNext()){
            Row row = it.next();
            String url = row.get(PropertyLoader.getProperty("table.crawler.url"));
            String links = row.get(PropertyLoader.getProperty("table.crawler.links"));
            Set<String> linkSet = DirectPageRank.efficientParsePageLinks(links);

            if(linkSet.isEmpty()){
                continue;
            }


            // build graph

            String hashedUrl = Hasher.hash(url);

            hashToUrl.put(hashedUrl, url);

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

                hashToUrl.put(hashedLink, link);

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


}
