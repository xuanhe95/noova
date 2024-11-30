package org.noova.indexer;

import org.noova.kvs.KVS;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class URLCache {
    static final Map<String, String> URL_ID_CACHE = new HashMap<>();

    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url-id.id");
    private static final String GLOBAL_COUNTER_TABLE = PropertyLoader.getProperty("table.counter");
    private static final String GLOBAL_COUNTER_KEY = PropertyLoader.getProperty("table.counter.global");

    private KVS kvs;
    private static URLCache instance = null;


    private URLCache(KVS kvs) {
        this.kvs = kvs;
    }

    public static URLCache getInstance(KVS kvs) {
        if (instance == null) {
            instance = new URLCache(kvs);
        }
        return instance;
    }


    private String getUrlId(String url) throws IOException {

        if(URL_ID_CACHE.containsKey(url)){
            System.out.println("URL_ID_CACHE contains url: " + url);
            return URL_ID_CACHE.get(url);
        }

        // use URL_ID_TABLE to find an url's corresponding urlID
        Row row = kvs.getRow(URL_ID_TABLE, Hasher.hash(url));
        if (row != null) {
            String id = row.get(URL_ID_VALUE);
            URL_ID_CACHE.put(url, id);
            return id;
        }

        // generate new id if url DNE
        synchronized (GLOBAL_COUNTER_KEY) {
            Row counterRow = kvs.getRow(GLOBAL_COUNTER_TABLE, GLOBAL_COUNTER_KEY);
            int globalCounter = (counterRow != null) ? Integer.parseInt(counterRow.get(URL_ID_VALUE)) : 0;

            // update global counter
            globalCounter++;
            if (counterRow == null) {
                counterRow = new Row(GLOBAL_COUNTER_KEY);
            }
            counterRow.put(URL_ID_VALUE, String.valueOf(globalCounter));
            kvs.putRow(GLOBAL_COUNTER_TABLE, counterRow);

            // store url and global counter to URL_ID_TABLE
            Row urlRow = new Row(Hasher.hash(url));
            urlRow.put(URL_ID_VALUE, String.valueOf(globalCounter));
            kvs.putRow(URL_ID_TABLE, urlRow);
            URL_ID_CACHE.put(url, String.valueOf(globalCounter));

            return String.valueOf(globalCounter);
        }
    }

    private void loadUrlId() throws IOException {
        var ids = kvs.scan(URL_ID_TABLE, null, null);
        ids.forEachRemaining(row -> {
            String id = row.get(URL_ID_VALUE);
            if(id == null){
                return;
            }
            URL_ID_CACHE.put(row.key(), id);
        });
    }


}
