package org.noova.gateway.service;


import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;

/**
 * @author Xuanhe Zhang
 */
public class SearchService implements IService {

    private static final Logger log = Logger.getLogger(SearchService.class);
    private static SearchService instance = null;

    private static final KVS KVS = new KVSClient(
            PropertyLoader.getProperty("kvs.host") +
                    ":" + PropertyLoader.getProperty("kvs.port"));

    private SearchService() {
    }

    public static SearchService getInstance() {
        if (instance == null) {
            instance = new SearchService();
        }
        return instance;
    }

    public List<String> searchByKeyword(String keyword, String startRow, String endRowExclusive) throws IOException {
        List<String> result = new ArrayList<>();

        Iterator<Row> it = KVS.scan(PropertyLoader.getProperty("table.crawler"), startRow, endRowExclusive);

        it.forEachRemaining(row -> {
            String value = row.get(PropertyLoader.getProperty("table.default.value"));
            if (value.contains(keyword)) {
                result.add(value);
            }
        });

        return result;
    }


    public Map<String, Set<Integer>> searchByKeyword(String keyword) throws IOException {

        keyword = Hasher.hash(keyword);

        Map<String, Set<Integer>> result = new HashMap<>();
        Row row = KVS.getRow(PropertyLoader.getProperty("table.index"), keyword);
        log.info("[search] Found row: " + keyword);
        row.columns().forEach(column -> {
            String urls = row.get(column);
            String[] urlArray = urls.split(",");
            for (String urlWithPosition : urlArray) {
                log.info("[search] Found URL with position: " + urlWithPosition);
                String rawPosition = urlWithPosition.substring(urlWithPosition.lastIndexOf(":") + 1);
                String normalizedUrl = urlWithPosition.substring(0, urlWithPosition.lastIndexOf(":"));
                String[] positions = rawPosition.split(" ");
                log.info("[search] Raw Position: " + rawPosition);
                log.info("[search] Found URL: " + normalizedUrl);
                Set<Integer> positionSet = new HashSet<>();

                for (String position : positions) {
                    log.info("[search] Adding position: " + position);
                    positionSet.add(Integer.parseInt(position));
                }

                result.put(normalizedUrl, positionSet);
            }
        });
        return result;
    }
/*
 *   TODO: predict the user's input based on the keyword
 *    For example, if the user types "c", the system should predict "cat", "car", "computer", etc.
 *    Possibly use a trie data structure to store the keywords
 */

    public List<String> predict(String keyword) {
        return new ArrayList<>();
    }
}
