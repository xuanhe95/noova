package org.noova.gateway.service;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.noova.gateway.markov.EfficientMarkovWord;
import org.noova.gateway.storage.StorageStrategy;
import org.noova.gateway.trie.Trie;
import org.noova.gateway.trie.TrieManager;
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

    private static Trie trie;

    private static SearchService instance = null;

    private static final StorageStrategy storageStrategy = StorageStrategy.getInstance();

    private static final TrieManager trieManager = TrieManager.getInstance();

    private static final boolean ENABLE_TRIE_CACHE = PropertyLoader.getProperty("cache.trie.enable").equals("true");

    private static final KVS KVS = storageStrategy.getKVS();


    private SearchService() {
        if (ENABLE_TRIE_CACHE) {
            try {
                //trie = TrieManager.getInstance().loadTrie(PropertyLoader.getProperty("table.index"));

                String trieName = PropertyLoader.getProperty("table.trie.default");

                if(storageStrategy.containsKey(
                        PropertyLoader.getProperty("table.trie"),
                        trieName,
                        "test"
                )){
                    log.info("[search] Trie found");
                    trie = trieManager.loadTrie(trieName);
                } else{
                    log.info("[search] Trie not found, building trie...");
                    trie = trieManager.buildTrie(PropertyLoader.getProperty("table.index"));
                    trieManager.saveTrie(trie, trieName);
                }

            } catch (IOException e) {
                log.error("[search] Error loading trie");
            }
        }

    }



    public static SearchService getInstance() {
        if (instance == null) {
            instance = new SearchService();
        }
        return instance;
    }

    public List<String> searchByKeyword(String keyword, String startRow, String endRowExclusive) throws IOException {

        List<String> result = new ArrayList<>();

        Iterator<Row> it = storageStrategy.scan(PropertyLoader.getProperty("table.crawler"), startRow, endRowExclusive);

        it.forEachRemaining(row -> {
            String value = row.get(PropertyLoader.getProperty("table.default.value"));
            if (value.contains(keyword)) {
                result.add(value);
            }
        });

        return result;
    }

    public Map<String, Set<Integer>> searchByKeyword(String keyword) throws IOException {

        if (keyword == null || keyword.isEmpty()) {
            log.warn("[search] Empty keyword");
            return new HashMap<>();
        }

//        if (ENABLE_TRIE_CACHE && trie != null && trie.contains(keyword)) {
//            log.info("[search] Found keyword in trie: " + keyword);
//            String urlsWithPositions = (String) trie.getValue(keyword);
//            return parseUrlWithPositions(urlsWithPositions);
//        }
//
        // keyword = Hasher.hash(keyword);

        Map<String, Set<Integer>> result = new HashMap<>();
        Row row = KVS.getRow(PropertyLoader.getProperty("table.index"), keyword);
        if (row == null) {
            log.warn("[search] No row found for keyword: " + keyword);
            return result;
        }
        log.info("[search] Found row: " + keyword);
        log.info("[search] Columns: " + row.columns());
        row.columns().forEach(column -> {
            result.putAll(parseUrlWithPositions(row.get(column)));
        });
        return result;
    }

    private Map<String, Set<Integer>> parseUrlWithPositions(String urlsWithPositions) {
        Map<String, Set<Integer>> result = new HashMap<>();
        String[] urlArray = urlsWithPositions.split(",");
        for (String urlWithPosition : urlArray) {
            String rawPosition = urlWithPosition.substring(urlWithPosition.lastIndexOf(":") + 1);
            String normalizedUrl = urlWithPosition.substring(0, urlWithPosition.lastIndexOf(":"));
            String[] positions = rawPosition.split(" ");
            Set<Integer> positionSet = new HashSet<>();

            for (String position : positions) {
                positionSet.add(Integer.parseInt(position));
            }

            result.put(normalizedUrl, positionSet);
        }
        return result;
    }

    public SortedMap<Double, String> sortByPageRank(Map<String, Set<Integer>> urlsWithPositions) {

        SortedMap<Double, String> result = new TreeMap<>(Comparator.reverseOrder());
        urlsWithPositions.forEach((normalizedUrl, positions) -> {
            try {
                double pagerank = getPagerank(normalizedUrl);
                log.info("[search] Found pagerank: " + pagerank + " for URL: " + normalizedUrl);
                result.put(pagerank, normalizedUrl);
            } catch (IOException e) {
                log.error("[search] Error getting pagerank for URL: " + normalizedUrl);
            }
        });

        return result;
    }

    public double getPagerank(String url) throws IOException {
        String hashedUrl = Hasher.hash(url);

        List<String> result = new ArrayList<>();
        // get the row from the pagerank table
        Row row = KVS.getRow(PropertyLoader.getProperty("table.pagerank"), hashedUrl);
        if (row == null) {
            log.warn("[search] No row found for URL: " + hashedUrl);
            return 0.0;
        }
        log.info("[search] Found row: " + hashedUrl);
        String rank = row.get("rank");
        if (rank == null) {
            log.warn("[search] No rank found for URL: " + hashedUrl);
            return 0.0;
        }

        return Double.parseDouble(rank);
    }

    /*
     * TODO: predict the user's input based on the keyword
     * For example, if the user types "c", the system should predict "cat", "car",
     * "computer", etc.
     * Possibly use a trie data structure to store the keywords
     */

    public List<String> predict(String prefix, int limit) {
        return trie.getWordsWithPrefix(prefix, limit);
    }

}
