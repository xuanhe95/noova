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
import org.noova.tools.Parser;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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

    private static final int totalDocuments = 815; // TBD, hardcoded for now


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
//        log.info("[search] Found row: " + keyword);
//        log.info("[search] Columns: " + row.columns());
//        row.columns().forEach(column -> {
//            result.putAll(parseUrlWithPositions(row.get(column)));
//        });

        Set<String> fromIds = row.columns();

        for (String fromUrlId : fromIds) {
            String texts = row.get(fromUrlId);
            if(texts.isEmpty()){
                continue;
            }
            Set<Integer> positions = parseUrlWithPositions(texts);
            if(result.containsKey(fromUrlId)){
                result.get(fromUrlId).addAll(positions);
            } else{
                result.put(fromUrlId, positions);
            }
        }
        return result;

    }

    private Set<Integer> parseUrlWithPositions(String urlsWithPositions) {
        Set<Integer> result = new HashSet<>();
        String rawPosition = urlsWithPositions.split(":")[0];
//        System.out.println(rawPosition);
        String[] positions = rawPosition.split("\\s+");
//        System.out.println(positions.length);
        for (String position : positions) {
            position = Parser.extractNumber(position);
            if (!position.isEmpty()) {
                result.add(Integer.parseInt(position));
            }
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



    public Map<String, Set<Integer>> searchByKeywords(String keywords) throws IOException {
        String[] terms = keywords.split("\\s+"); //TBD: auto correction logic and security checks
        Map<String, Set<Integer>> aggregatedResults = new HashMap<>();

        for (String term : terms) {
            Map<String, Set<Integer>> urlsForTerm = searchByKeyword(term);
            urlsForTerm.forEach((url, positions) -> {
                aggregatedResults.computeIfAbsent(url, k -> new HashSet<>()).addAll(positions);
            });
        }

        return aggregatedResults;
    }
    public Map<String, Double> calculateQueryTFIDF(String query) throws IOException {
        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));
        Map<String, Double> queryTfidf = new HashMap<>();

        for (String term : queryTokens) {
            int df = 0;// need to update
            if (df > 0) {
                double idf = Math.log((double) totalDocuments / (1 + df));
                double tf = Collections.frequency(queryTokens, term) / (double) queryTokens.size();
//                log.info("[search] tf: "+ tf + " idf: " +idf + " df: "+ df);
                queryTfidf.put(term, tf * idf);
            }
        }
        return queryTfidf;
    }

    public Map<String, Double> calculateDocumentTFIDF(String url, Set<Integer> positions) throws IOException {
        Map<String, Double> docTfidf = new HashMap<>();

        String hashedUrl = Hasher.hash(url);
        Row row = KVS.getRow(PropertyLoader.getProperty("table.crawler"), hashedUrl);
        log.info("[search] Found row: " + hashedUrl + " for URL: " + url + "row: " + row);
        if (row == null) {
            log.warn("[search] calculateDocumentTFIDF No content found for URL: " + url);
            return docTfidf;
        }

        String pageContent = row.get("page");
        if (pageContent == null) {
            log.warn("[search] calculateDocumentTFIDF No page content in 'page' column for URL: " + url);
            return docTfidf;
        }

        List<String> terms = Arrays.asList(pageContent.toLowerCase().split("\\s+"));
        int docLength = terms.size();

        Map<String, Long> termFrequencies = terms.stream().collect(Collectors.groupingBy(term -> term, Collectors.counting()));

        for (Map.Entry<String, Long> entry : termFrequencies.entrySet()) {
            String term = entry.getKey();
            long tf = entry.getValue();

            int df = 0;// need to udpate
            if (df > 0) {
                double idf = Math.log((double) totalDocuments / (1 + df));
                double tfidfValue = (tf / (double) docLength) * idf;
                docTfidf.put(term, tfidfValue);
                log.info("[search] calculateDocumentTFIDF tf: "+ tf + " docLength: " + docLength + " idf: " +idf + " df: "+ df);

            }
        }

        return docTfidf;
    }

    public double cosineSimilarity(Map<String, Double> vec1, Map<String, Double> vec2) {
        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (String term : vec1.keySet()) {
            dotProduct += vec1.get(term) * vec2.getOrDefault(term, 0.0);
            norm1 += Math.pow(vec1.get(term), 2);
        }

        for (double value : vec2.values()) {
            norm2 += Math.pow(value, 2);
        }

        if (norm1 == 0 || norm2 == 0) return 0.0;
        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

//    private int documentFrequency(String term) throws IOException {
//        Row row = KVS.getRow(PropertyLoader.getProperty("table.index"), term);
//        Map<String, Set<Integer>> result = new HashMap<>();
//
//        row.columns().forEach(column -> {
//            result.putAll(parseUrlWithPositions(row.get(column)));
//        });
//
////        log.info("[search] Found row: " + row + " result: " + result + " number of file: " + result.size());
//        return result.size();
//    }

    public String getPageContent(String url) throws IOException {
        String hashedUrl = Hasher.hash(url);
        Row row = KVS.getRow(PropertyLoader.getProperty("table.crawler"), hashedUrl);
        log.info("[search] getPageContent: " + hashedUrl + " for URL: " + url + "row: " + row);

        if (row == null) {
            return "";
        }else{
            return row.get("page");
        }
    }

    public String ExtractContextSnippet(String content, Set<Integer> positions, int wordLimit) {
        if (content == null || content.isEmpty() || positions.isEmpty()) {
            return "";
        }

        // Use the first position from the set as the starting point
        int position = positions.iterator().next();

        // Tokenize the page content into words
        String[] words = content.split("\\s+");

        // Calculate start and end indexes for a 30-word window around the position
        int start = Math.max(0, position - wordLimit / 2);
        int end = Math.min(words.length, start + wordLimit);

        // Build the snippet string
        StringBuilder snippet = new StringBuilder();
        for (int i = start; i < end; i++) {
            if (positions.contains(i)) {
                snippet.append("<b>").append(words[i]).append("</b>").append(" ");
            } else {
                snippet.append(words[i]).append(" ");
            }
        }

        return snippet.toString().trim();
    }

    public List<String> getAutocompleteSuggestions(String inputWords, int limit) {
        return trie.getWordsWithPrefix(inputWords,10);
    }


    public String getSnapshot(String url) throws IOException {
        String hashedUrl = Hasher.hash(url);
        Row row = KVS.getRow(PropertyLoader.getProperty("table.crawler"), hashedUrl);
        if (row == null) {
            log.warn("[search] No row found for URL: " + hashedUrl);
            return null;
        }
        log.info("[search] Found row: " + hashedUrl);
        return row.get(PropertyLoader.getProperty("table.crawler.page"));
    }

    public List<String> getImages(String keyword) throws IOException {
        Row row = KVS.getRow(PropertyLoader.getProperty("table.index"), keyword);

        if (row == null || row.get("images") == null || row.get("images").isEmpty()) {
            log.warn("[search] No images found for keyword: " + keyword);
            return Collections.emptyList();
        }

        String imagesColumn = row.get("images");
        return Arrays.asList(imagesColumn.split(","));
    }
}
