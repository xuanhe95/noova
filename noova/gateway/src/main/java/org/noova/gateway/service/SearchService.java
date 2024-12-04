package org.noova.gateway.service;

//import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.noova.gateway.storage.StorageStrategy;
import org.noova.gateway.trie.Trie;
import org.noova.gateway.trie.TrieManager;
import org.noova.kvs.KVS;
import org.noova.kvs.Row;
import org.noova.tools.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    private static final String PGRK_TABLE = PropertyLoader.getProperty("table.pagerank");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String ID_URL_TABLE = PropertyLoader.getProperty("table.id-url");
    private static final Map<String, String> ID_URL_CACHE = new ConcurrentHashMap<>();

    private static final KVS KVS = storageStrategy.getKVS();

    private static final int totalDocuments = 8692; // TBD, hardcoded for now

    private static final Double DOC_TF_NORMALIZE_FACTOR = 0.4; // either 0.4 or 0.5


    private SearchService() {
        if (ENABLE_TRIE_CACHE) {
            try {
                //trie = TrieManager.getInstance().loadTrie(PropertyLoader.getProperty("table.index"));

                String trieName = PropertyLoader.getProperty("table.trie.default");

                if(storageStrategy.containsKey(
                        PropertyLoader.getProperty("table.trie"),
                        trieName,
                        PropertyLoader.getProperty("table.default.value")
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

    public void preloadIDUrlCache() throws IOException {
        Iterator<Row> rows = KVS.scan(ID_URL_TABLE, null, null);
        while (rows.hasNext()) {
            Row row = rows.next();
            String fromUrlId = row.key();
            String hashedUrl = row.get("value");
            if (fromUrlId != null && hashedUrl != null) {
                ID_URL_CACHE.put(fromUrlId, hashedUrl);
            }
        }
        log.info("[cache] Preloaded ID_URL_TABLE with " + ID_URL_CACHE.size() + " entries.");
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

    public String getTitle(String hashedUrl) throws IOException {
        Row row = KVS.getRow(PROCESSED_TABLE, hashedUrl);
        if(row==null) return "TBD";
        return CapitalizeTitle.toTitleCase(row.get("title"));
    }

    public double calculateTitleMatchScore(String query, String title) throws IOException {
        // [ranking] - title weight

        if (title == null || title.isEmpty()) {
            return 0.0;
        }

        List<String> titleTokens = Arrays.asList(title.toLowerCase().split("\\s+"));
        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));

        // count match
        long matchCount = queryTokens.stream()
                .filter(titleTokens::contains)
                .count();

        // norm weight by title size
        return (double) matchCount / titleTokens.size();
    }

    public String getOGDescription(String hashedUrl) throws IOException {
        Row row = KVS.getRow(PROCESSED_TABLE, hashedUrl);
        if (row == null) return "";
        return row.get("description");
    }

    public double calculateOGDescriptionMatch(String hashedUrl, String query) throws IOException {
        String ogDescription = getOGDescription(hashedUrl);
        if (ogDescription == null || ogDescription.isEmpty()) return 0.0;

        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));
        List<String> descriptionTokens = Arrays.asList(ogDescription.toLowerCase().split("\\s+"));

        long matchCount = queryTokens.stream()
                .filter(descriptionTokens::contains)
                .count();

        return (double) matchCount / queryTokens.size();
    }


    public Map<String, Set<Integer>> searchByKeyword(String keyword) throws IOException {
        // uses HASHED_URL as key

        preloadIDUrlCache(); // preload url id cache in mem

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
        Row row = KVS.getRow(INDEX_TABLE, keyword);
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
//            String hashedUrl = KVS.getRow(ID_URL_TABLE, fromUrlId).get("value");
            String hashedUrl = ID_URL_CACHE.get(fromUrlId);
            if(texts.isEmpty()){
                continue;
            }
            Set<Integer> positions = parseUrlWithPositions(texts);
            if(result.containsKey(hashedUrl)){
                result.get(hashedUrl).addAll(positions);
            } else{
                result.put(hashedUrl, positions);
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

    public double getPagerank(String hashedUrl) throws IOException {
//        String hashedUrl = Hasher.hash(url);

        List<String> result = new ArrayList<>();
        // get the row from the pagerank table
        Row row = KVS.getRow(PGRK_TABLE, hashedUrl);
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

    static class Node implements Comparable<Node> {
        int value; // 当前距离
        int index; // 列表索引
        int listIndex; // 所属列表的索引

        Node(int value, int index, int listIndex) {
            this.value = value;
            this.index = index;
            this.listIndex = listIndex;
        }

        @Override
        public int compareTo(Node other) {
            return Integer.compare(this.value, other.value); // 按值升序排列
        }
    }

    public static List<Integer> getBestPosition(List<String> words, Map<String, Set<Integer>> positions, int maxSpan) {
        List<List<Integer>> allPositions = new ArrayList<>();
        PriorityQueue<Node> pq = new PriorityQueue<>();

        // 初始化所有位置列表，并将每个列表的第一个元素加入优先队列
        for (String word : words) {
            if (positions.containsKey(word)) {
                List<Integer> posList = new ArrayList<>(positions.get(word));
                Collections.sort(posList);
                allPositions.add(posList);
                pq.offer(new Node(posList.get(0), 0, allPositions.size() - 1));
            } else {
                return new ArrayList<>(); // 如果某个单词没有位置，返回空
            }
        }

        int minDistance = Integer.MAX_VALUE;
        int maxPos = Integer.MIN_VALUE; // 记录当前窗口的最大位置
        List<Integer> result = new ArrayList<>();

        // 初始化最大位置
        for (List<Integer> posList : allPositions) {
            maxPos = Math.max(maxPos, posList.get(0));
        }

        while (true) {
            Node minNode = pq.poll(); // 获取当前最小位置
            int minPos = minNode.value;

            // 更新最小距离和结果
            int currentDistance = maxPos - minPos;
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                result.clear();
                for (Node node : pq) {
                    result.add(allPositions.get(node.listIndex).get(node.index));
                }
                result.add(minPos); // 加入当前最小值
            }

            // 提前终止条件
            if (currentDistance <= maxSpan) {
                return result;
            }

            // 移动指针到下一个位置
            List<Integer> currentList = allPositions.get(minNode.listIndex);
            if (minNode.index + 1 < currentList.size()) {
                int nextValue = currentList.get(minNode.index + 1);
                pq.offer(new Node(nextValue, minNode.index + 1, minNode.listIndex));
                maxPos = Math.max(maxPos, nextValue); // 更新最大位置
            } else {
                break; // 如果某个列表已耗尽，结束
            }
        }

        return result;
    }

    public List<Integer> getBestPosition(List<String> words, Map<String, Set<Integer>> positions){
        List<Integer> result = new ArrayList<>();

        // 将 positions 转换成列表形式，方便操作
        List<List<Integer>> allPositions = new ArrayList<>();
        for (String word : words) {
            if (positions.containsKey(word)) {
                System.out.println("Word: " + word);
                List<Integer> posList = new ArrayList<>(positions.get(word));
                Collections.sort(posList);
                allPositions.add(posList);
            } else {
                return result;
            }
        }

        // 使用指针方法查找最小跨度
        int[] indices = new int[allPositions.size()]; // 指针数组，表示每个列表当前访问的位置
        int minDistance = Integer.MAX_VALUE;

        while (true) {
            int minPos = Integer.MAX_VALUE;
            int maxPos = Integer.MIN_VALUE;
            int minListIndex = -1;

            // 找到当前指针对应的最小和最大位置
            for (int i = 0; i < allPositions.size(); i++) {
                int currentPos = allPositions.get(i).get(indices[i]);
                if (currentPos < minPos) {
                    minPos = currentPos;
                    minListIndex = i;
                }
                if (currentPos > maxPos) {
                    maxPos = currentPos;
                }
            }

            // 更新最小距离
            int currentDistance = maxPos - minPos;
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                result.clear();
                for (int i = 0; i < allPositions.size(); i++) {
                    result.add(allPositions.get(i).get(indices[i]));
                }
            }

            // 移动指针：最小位置的指针前进
            indices[minListIndex]++;
            if (indices[minListIndex] >= allPositions.get(minListIndex).size()) {
                break; // 如果某个列表到达末尾，退出循环
            }
        }

        return result;

    }

    public Map<String, List<Integer>> calculateSortedPosition(List<String> words) {
        // Map of URL -> word -> positions
        Map<String, Map<String, Set<Integer>>> urlsForWordMap = new HashMap<>();

        // Populate urlsForWordMap
        words.forEach(word -> {
            try {
                // Get all URLs for this word
                Map<String, Set<Integer>> urlsForWord = searchByKeyword(word);

                // Process URLs and positions for this word
                urlsForWord.forEach((url, positions) -> {
                    urlsForWordMap
                            .computeIfAbsent(url, k -> new HashMap<>())
                            .put(word, positions);
                });

            } catch (IOException e) {
                log.error("[search] Error searching by keyword: " + word, e);
            }
        });

        // Find valid URLs, calculate best positions and their spans
        Map<String, Integer> urlToSpan = new HashMap<>();
        Map<String, List<Integer>> urlToPositions = new HashMap<>();

        urlsForWordMap.entrySet().stream()
                .filter(entry -> entry.getValue().keySet().containsAll(words)) // Filter URLs containing all words
                .forEach(entry -> {
                    String url = entry.getKey();
                    Map<String, Set<Integer>> valid = entry.getValue();
                    List<Integer> best = getBestPosition(words, valid, words.size() + 2);

                    // Calculate span
                    int span = best.isEmpty() ? Integer.MAX_VALUE : Collections.max(best) - Collections.min(best);

                    urlToSpan.put(url, span);
                    urlToPositions.put(url, best);
                });

        // Sort the map by span and return a LinkedHashMap to preserve the order
        return urlToSpan.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()) // Sort by span
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> urlToPositions.get(entry.getKey()),
                        (e1, e2) -> e1,
                        LinkedHashMap::new // Maintain insertion order
                ));
    }
//    public Map<String, List<Integer>> calculatePosition(List<String> words) {
//        // Final result: URL -> best positions
//        Map<String, List<Integer>> urlToPositions = new HashMap<>();
//
//        // Map of URL -> word -> positions
//        Map<String, Map<String, Set<Integer>>> urlsForWordMap = new HashMap<>();
//
//        words.forEach(word -> {
//            try {
//                // Get all URLs for this word
//                Map<String, Set<Integer>> urlsForWord = searchByKeyword(word);
//
//                // Process URLs and positions for this word
//                urlsForWord.forEach((url, positions) -> {
//                    urlsForWordMap
//                            .computeIfAbsent(url, k -> new HashMap<>())
//                            .put(word, positions);
//                });
//
//            } catch (IOException e) {
//                log.error("[search] Error searching by keyword: " + word, e);
//            }
//        });
//
//        // Find valid URLs and calculate best positions
//        urlsForWordMap.entrySet().stream()
//                .filter(entry -> entry.getValue().keySet().containsAll(words)) // Filter URLs containing all words
//                .forEach(entry -> {
//                    String url = entry.getKey();
//                    Map<String, Set<Integer>> valid = entry.getValue();
//                    List<Integer> best = getBestPosition(words, valid, words.size() + 2);
//                    urlToPositions.put(url, best);
//                });
//
//        return urlToPositions;
//    }
    public Map<String, Set<Integer>> searchByKeywords(String keywords) throws IOException {

        if(keywords == null || keywords.isEmpty()){
            log.warn("[search] Empty keywords");
            return new HashMap<>();
        }

        String[] terms = keywords.toLowerCase().split("\\s+"); //TBD: auto correction logic and security checks
        Map<String, Set<Integer>> aggregatedResults = new HashMap<>();

        for (String term : terms) {
            Map<String, Set<Integer>> urlsForTerm = searchByKeyword(term);
            urlsForTerm.forEach((url, positions) -> {
                aggregatedResults.computeIfAbsent(url, k -> new HashSet<>()).addAll(positions);
            });
        }

        return aggregatedResults;
    }

    private int documentFrequency(String term) throws IOException {
        Row row = KVS.getRow(INDEX_TABLE, term);
        if (row == null) { // if term not found term, return 0 for df
            return 0;
        }

        System.out.println("term's row size: "+row.columns().size());
        return row.columns().size(); // number of cols (url)
    }

    public Map<String, Double> calculateQueryTF(String query) throws IOException {
        // [Prof approach] - apply idf to query, NOT to both query and doc; query does not need normalization
        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));
        Map<String, Double> queryTf = new HashMap<>(); // term:tf

        for (String term : queryTokens) {
            int df = documentFrequency(term);
            if(df <= 0) {
                queryTf.put(term , 0.0);
            } else {
                double idf = Math.log10((double) totalDocuments / df); // inverse N/n_i
                double tf = Collections.frequency(queryTokens, term); // freq of a word in query, no need norm
                System.out.println("[search] tf: "+ tf + " idf: " +idf + " df: "+ df);
                queryTf.put(term, tf * idf);
            }
        }
        return queryTf;
    }

    public Map<String, Double> calculateDocumentTF(String hashedUrl, String query) throws IOException {
        // [Prof approach] - pick a doc to score, normalize tf in a doc [a + (1 - a) * (freq of term / max freq of a word in doc)]

        Map<String, Double> docTf = new HashMap<>(); // doc_term:norm_tf
        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));

        // convert hashurl to id
        String urlID = KVS.getRow("pt-urltoid", hashedUrl).get("value");
        if (urlID == null) {
            log.error("[calculateDocumentTF] URL ID not found for hashed URL: " + hashedUrl);
            return docTf;
        }
        log.info("[calculateDocumentTF] Processing URL ID: " + urlID);

//        String hashedUrl = Hasher.hash(url); // col
        double tfSquareSum = 0.0;

        // 1. scan index row once to update maxFreq and corr query_term freq
        for (String queryTerm : queryTokens) {
            Row row = KVS.getRow(INDEX_TABLE, queryTerm);
            if (row == null) continue;

            String cellValue = row.get(urlID);
            if (cellValue == null) continue;

            String[] parts = cellValue.split(":");
            if (parts.length < 2) continue;

            String[] freqParts = parts[1].split("/");
            if (freqParts.length < 2) continue;

            double frequency = Double.parseDouble(freqParts[0]);
            tfSquareSum += Math.pow(frequency, 2);
        }

        double normalizationFactor = Math.sqrt(tfSquareSum);
        log.info("[calculateDocumentTF] Normalization factor for URL ID: " + urlID + " is: " + normalizationFactor);

        // normalize freq
        for (String queryTerm : queryTokens) {
            Row row = KVS.getRow(INDEX_TABLE, queryTerm);
            if (row == null) continue;

            String cellValue = row.get(urlID);
            if (cellValue == null) continue;

            String[] parts = cellValue.split(":");
            if (parts.length < 2) continue;

            String[] freqParts = parts[1].split("/");
            if (freqParts.length < 2) continue;

            double frequency = Double.parseDouble(freqParts[0]);
            double normalizedTf = frequency / normalizationFactor;
            docTf.put(queryTerm, normalizedTf);
        }
        return docTf;


        // 2. parallel?
//        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//        List<Future<Map.Entry<String, Double>>> futures = new ArrayList<>();
//
//        for (String queryTerm : queryTokens) {
//            futures.add(executor.submit(() -> {
//                Row row = KVS.getRow(INDEX_TABLE, queryTerm);
//                if (row == null) return null;
//
//                String cellValue = row.get(urlID); // col is url id
//                log.info("[calculateDocumentTF] Cell value for term: " + queryTerm + " in URL ID: " + urlID + " is: " + cellValue);
//
//                if (cellValue == null) return null;
//
//                String[] parts = cellValue.split(":");
//                if (parts.length < 2) {
//                    log.error("[calculateDocumentTF] Invalid cell value format: " + cellValue);
//                    return null;
//                }
//
//                String[] freqParts = parts[1].split("/");
//                if (freqParts.length < 2) {
//                    log.error("[calculateDocumentTF] Invalid frequency format: " + parts[1]);
//                    return null;
//                }
//
//                double frequency = Double.parseDouble(freqParts[0]);
//                System.out.println("frequency: "+frequency);
//                return Map.entry(queryTerm, frequency);
//            }));
//        }
//
//        executor.shutdown();
//        for (Future<Map.Entry<String, Double>> future : futures) {
//            try {
//                Map.Entry<String, Double> result = future.get();
//                if (result != null) {
//                    docTf.put(result.getKey(), result.getValue());
//                    maxFrequency = Math.max(maxFrequency, result.getValue());
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        if (maxFrequency == 0.0) {
//            log.warn("[calculateDocumentTF] Max frequency is zero for URL ID: " + urlID);
//            return docTf;
//        }
//
//        log.info("[calculateDocumentTF] Max frequency for URL ID: " + urlID + " is: " + maxFrequency);
    }

    public double calculateTFIDF(Map<String, Double> queryTF, Map<String, Double> docTf){
        // [Prof approach] - final tf-idf is simply the dot product of query and doc tfidf
        double tfidfScore= 0.0;

        for (Map.Entry<String, Double> entry : queryTF.entrySet()) {
            String term = entry.getKey();
            double queryWeight = entry.getValue();

            log.info("[calculateTFIDF] term: " + term + " doc tf is: " + queryWeight);


            if (docTf.containsKey(term)) {
                double docWeight = docTf.get(term);
                tfidfScore += queryWeight * docWeight;
            }
        }

        log.info("[calculateTFIDF] tfidfScore: " + tfidfScore );
        return tfidfScore;
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

    public Map<String, Double> calculateDocumentTFIDF(String hashedUrl, Set<Integer> positions) throws IOException {
        Map<String, Double> docTfidf = new HashMap<>();

//        String hashedUrl = Hasher.hash(url);
        Row row = KVS.getRow(PropertyLoader.getProperty("table.crawler"), hashedUrl);
        log.info("[search] Found row: " + hashedUrl + " for URL: " + hashedUrl + "row: " + row);
        if (row == null) {
            log.warn("[search] calculateDocumentTFIDF No content found for URL: " + hashedUrl);
            return docTfidf;
        }

        String pageContent = row.get("page");
        if (pageContent == null) {
            log.warn("[search] calculateDocumentTFIDF No page content in 'page' column for URL: " + hashedUrl);
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


    public String getSnapshot(String hashedUrl) throws IOException {
//        String hashedUrl = Hasher.hash(url);
        Row row = KVS.getRow(PropertyLoader.getProperty("table.crawler"), hashedUrl);
        if (row == null) {
            log.warn("[search] No row found for URL: " + hashedUrl);
            return null;
        }
        log.info("[search] Found row: " + hashedUrl);
        return row.get(PropertyLoader.getProperty("table.crawler.page"));
    }

    public List<String> getImages(String keyword) throws IOException {
        Row row = KVS.getRow(INDEX_TABLE, keyword);

        if (row == null || row.get("images") == null || row.get("images").isEmpty()) {
            log.warn("[search] No images found for keyword: " + keyword);
            return Collections.emptyList();
        }

        String imagesColumn = row.get("images");
        return Arrays.asList(imagesColumn.split(","));
    }
}
