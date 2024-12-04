package org.noova.gateway.service;

//import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.noova.gateway.storage.StorageStrategy;
import org.noova.gateway.trie.Trie;
import org.noova.gateway.trie.TrieManager;
import org.noova.kvs.KVS;
import org.noova.kvs.Row;
import org.noova.tools.*;

import java.io.IOException;
import java.net.URL;
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
    private static final String INDEX_IMAGE_TABLE = PropertyLoader.getProperty("table.image");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String ID_URL_TABLE = PropertyLoader.getProperty("table.id-url");
    private static final Map<String, String> ID_URL_CACHE = new ConcurrentHashMap<>();

    private static final KVS KVS = storageStrategy.getKVS();

    private static final int totalDocuments = 8692; //! TBD, hardcoded for now

    private static final Map<String, Double> PAGE_RANK_CACHE = new ConcurrentHashMap<>();
    private final Map<String, Row> processedTableCache = new ConcurrentHashMap<>(); // recent cache



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

        // preload caches
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(this::preloadIDUrlCache);
        executor.submit(this::preloadPageRankCache);
        executor.shutdown();

    }



    public static SearchService getInstance() {
        if (instance == null) {
            instance = new SearchService();
        }
        return instance;
    }

//    public void preloadIDUrlCache() {
//        Iterator<Row> rows = null;
//        try {
//            rows = KVS.scan(ID_URL_TABLE , null , null);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void preloadIDUrlCache() {
        try {
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
        } catch (IOException e) {
            log.error("[cache] Error preloading ID_URL_TABLE", e);
            throw new RuntimeException(e);
        }
    }

    public void preloadPageRankCache() {
        try {
            Iterator<Row> rows = KVS.scan(PGRK_TABLE, null, null);
            while (rows.hasNext()) {
                Row row = rows.next();
                String url = row.key();
                String rankStr = row.get("rank");
                if (url != null && rankStr != null) {
                    PAGE_RANK_CACHE.put(url, Double.parseDouble(rankStr));
                }
            }
            log.info("[cache] Preloaded PAGERANK_TABLE with " + PAGE_RANK_CACHE.size() + " entries.");
        } catch (IOException e) {
            log.error("[cache] Error preloading PGRK_TABLE", e);
            throw new RuntimeException(e);
        }
    }

    public Row getCachedRow(String tableName, String key) {
        return processedTableCache.computeIfAbsent(key, k -> {
            try {
                return KVS.getRow(tableName, k);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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

    private List<Integer> parseUrlWithSortedPositions(String urlsWithPositions) {
        List<Integer> result = new ArrayList<>();
        String rawPosition = urlsWithPositions.split(":")[0];
//        System.out.println(rawPosition);
        String[] positions = rawPosition.split("\\s+");
//        System.out.println(positions.length);
        for (String position : positions) {
            position = Parser.extractNumber(position);
            assert position != null;
            if (!position.isEmpty()) {
                result.add(Integer.parseInt(position));
            }
        }
        return result;
    }

    public Map<String, Map<String, List<Integer>>> searchByKeywordsIntersection(List<String> keywords) throws IOException {
        // uses HASHED_URL as key
        if (keywords == null || keywords.isEmpty()) {
            log.warn("[search] Empty keyword");
            return new HashMap<>();
        }


        Map<String, Map<String, List<Integer>>> allResults = new HashMap<>();

        Map<String, Row> cache = new HashMap<>();

        Set<String> allFromIds = null;

        for (String keyword : keywords) {
            Row row = KVS.getRow(INDEX_TABLE, keyword);
            if (row == null) {
                log.warn("[search] No row found for keyword: " + keyword);
                continue;
            }

            cache.put(keyword, row);

            if (row.columns() != null) {
                if (allFromIds == null) {
                    allFromIds = row.columns();
                } else {
                    allFromIds.retainAll(row.columns());
                }
            }
        }

        if (allFromIds == null || allFromIds.isEmpty()) {
            log.warn("[search] No row found for keyword: " + keywords);
            return allResults;
        }
        for (String fromUrlId : allFromIds) {

            Map<String, List<Integer>> result = new HashMap<>();
            for (String keyword : keywords) {
                Row row = cache.getOrDefault(keyword, null);

                if(row == null){
                    continue;
                }

                String texts = row.get(fromUrlId);
                if (texts.isEmpty()) {
                    continue;
                }
                List<Integer> positions = parseUrlWithSortedPositions(texts);
                result.put(keyword, positions);
            }
            allResults.put(fromUrlId, result);
        }
        return allResults;

    }




    public Map<String, Set<Integer>> searchByKeyword(String keyword) throws IOException {
        // uses HASHED_URL as key

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
            String hashedUrl = KVS.getRow(ID_URL_TABLE, fromUrlId).get("value");
//            String hashedUrl = ID_URL_CACHE.get(fromUrlId);
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

//        return PAGE_RANK_CACHE.getOrDefault(hashedUrl, 0.0);
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

    public static List<Integer> getBestPositionWithSorted(List<String> words, Map<String, List<Integer>> positions, int spanTolerance, int spanLimit) {
        List<List<Integer>> allPositions = new ArrayList<>();
        PriorityQueue<Node> pq = new PriorityQueue<>();

        // 初始化所有位置列表，并将每个列表的第一个元素加入优先队列
        for (String word : words) {
            if (positions.containsKey(word)) {
                List<Integer> posSortedList = positions.get(word);

                allPositions.add(posSortedList);
                pq.offer(new Node(posSortedList.get(0), 0, allPositions.size() - 1));
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
            if (currentDistance <= spanTolerance) {
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

        if(minDistance> spanLimit){
            return new ArrayList<>();
        }

        return result;
    }



    public static List<Integer> getBestPosition(List<String> words, Map<String, Set<Integer>> positions, int spanTolerance, int spanLimit) {
        List<List<Integer>> allPositions = new ArrayList<>();
        PriorityQueue<Node> pq = new PriorityQueue<>();

        // 初始化所有位置列表，并将每个列表的第一个元素加入优先队列
        for (String word : words) {
            if (positions.containsKey(word)) {
                Set<Integer> posSet = positions.get(word);






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
            if (currentDistance <= spanTolerance) {
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

        if(minDistance> spanLimit){
            return new ArrayList<>();
        }

        return result;
    }

    public static List<Integer> getBestPositionV2(List<String> words, Map<String, Set<Integer>> positions, int maxSpan) {
        List<List<Integer>> allPositions = new ArrayList<>();
        PriorityQueue<Node> pq = new PriorityQueue<>();

        int globalMinPos = Integer.MAX_VALUE; // 全局最小位置

        // 初始化所有位置列表，并将最小位置加入优先队列
        for (String word : words) {
            if (positions.containsKey(word)) {
                List<Integer> posList = new ArrayList<>(positions.get(word));
                Collections.sort(posList);
                allPositions.add(posList);
                pq.offer(new Node(posList.get(0), 0, allPositions.size() - 1));

                // 更新全局最小位置
                globalMinPos = Math.min(globalMinPos, posList.get(0));
            } else {
                return new ArrayList<>(); // 如果某个单词没有位置，返回空
            }
        }

        int minDistance = Integer.MAX_VALUE;
        int maxPos = globalMinPos; // 从全局最小位置开始
        List<Integer> result = new ArrayList<>();

        // 初始化最大位置
        for (List<Integer> posList : allPositions) {
            maxPos = Math.max(maxPos, posList.get(0));
        }

        while (!pq.isEmpty()) {
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

            // 提前终止条件：如果最小跨度满足要求
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

    public Map<String, List<Integer>> calculateSortedPosition(List<String> words) throws IOException {

        System.out.println("words: " + words);

        Map<String, List<Integer>> result = new HashMap<>();
        Map<String, Integer> urlToSpan = new HashMap<>();
        if (words == null || words.isEmpty()) {
            return new HashMap<>();
        }

        // url -> word -> positions
        Map<String, Map<String, List<Integer>>> results = searchByKeywordsIntersection(words);
        if (results == null || results.isEmpty()) {
            return new HashMap<>();
        }

        for (String url : results.keySet()) {
            Map<String, List<Integer>> wordPositionPair = results.get(url);
            if (wordPositionPair == null || wordPositionPair.isEmpty()) {
                continue;
            }

            List<Integer> best = getBestPositionWithSorted(words, wordPositionPair, words.size() + 4, 20);

            if(best.isEmpty()){
                continue;
            }

            int span = Collections.max(best) - Collections.min(best);
            System.out.println("best: " + best + ", span: " + span);

            result.put(url, best);
            urlToSpan.put(url, span);

            result.put(url, best);
        }

        return urlToSpan.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()) // 按 span 值排序
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> result.get(entry.getKey()), // 获取对应的最佳位置
                        (e1, e2) -> e1, // 合并策略
                        LinkedHashMap::new // 使用 LinkedHashMap 保持排序后的顺序
                ));
    }

    /*
=======

        //List<Integer> best = getBestPosition(words, results.get(words.get(0)), words.size() + 2);



//        for(String word : results.keySet()){
//            Map<String, Set<Integer>> urlsForWord = results.get(word);
//            if(urlsForWord == null || urlsForWord.isEmpty()){
//                continue;
//            }
//
//            List<Integer> best = getBestPosition(words, urlsForWord, words.size() + 2);
//
//        }

//        Map<String, Integer> urlToSpan = new HashMap<>();
//        Map<String, List<Integer>> urlToPositions = new HashMap<>();

//        urlsForWordMap.entrySet().parallelStream()
//                .filter(entry -> entry.getValue().keySet().containsAll(words)) // Filter URLs containing all words
//                .forEach(entry -> {
//                    String url = entry.getKey();
//                    Map<String, Set<Integer>> valid = entry.getValue();
//                    List<Integer> best = getBestPosition(words, valid, words.size() + 2);
//
//                    // Calculate span
//                    int span = best.isEmpty() ? Integer.MAX_VALUE : Collections.max(best) - Collections.min(best);
//
//                    // Use thread-safe collections
//                    urlToSpan.put(url, span);
//                    urlToPositions.put(url, best);
//                });

//        // Sort the map by span and return a LinkedHashMap to preserve the order
//        return urlToSpan.entrySet().stream()
//                .filter(entry -> urlToPositions.containsKey(entry.getKey())) // 确保 key 存在
//                .filter(entry -> urlToPositions.get(entry.getKey()) != null) // 确保值非空
//                .filter(entry -> !urlToPositions.get(entry.getKey()).isEmpty()) // 确保列表非空
//                .sorted(Map.Entry.comparingByValue()) // 按跨度排序
//                .collect(Collectors.toMap(
//                        Map.Entry::getKey,
//                        entry -> urlToPositions.get(entry.getKey()), // 安全获取值
//                        (e1, e2) -> e1,
//                        LinkedHashMap::new // 保持顺序
//                ));
//    }
>>>>>>> 3bbdb4acc (make position better)
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
    */

    public String generateSnippetFromPositions(String content, List<Integer> positions, int wordLimit) {
        // use best pos to get the snippet for highlight

        if (content == null || content.isEmpty() ||positions==null|| positions.isEmpty()) {
            return "";
        }
        String[] words = content.split("\\s+");
        int start = Math.max(0, positions.get(0) - wordLimit / 2);
        int end = Math.min(words.length, start + wordLimit);

        return Arrays.stream(Arrays.copyOfRange(words, start, end))
                .map(word -> positions.contains(Arrays.asList(words).indexOf(word)) ? "<b>" + word + "</b>" : word)
                .collect(Collectors.joining(" "));
    }


    private int documentFrequency(String term) throws IOException {
        Row row = KVS.getRow(INDEX_TABLE, term);
        if (row == null) { // if term not found term, return 0 for df
            return 0;
        }

//        System.out.println("term's row size: "+row.columns().size());
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
//                System.out.println("[search] tf: "+ tf + " idf: " +idf + " df: "+ df);
                queryTf.put(term, tf * idf);
            }
        }
        return queryTf;
    }

    public Map<String, Double> calculateDocumentTF(String hashedUrl, String query) throws IOException {
        // [Prof approach] - pick a doc to score, normalize tf in a doc freq/sqrt(w_i^2)

        Map<String, Double> docTf = new HashMap<>(); // doc_term:norm_tf
        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));

        // convert hashurl to id
        String urlID = KVS.getRow("pt-urltoid", hashedUrl).get("value");
        if (urlID == null) {
//            log.error("[calculateDocumentTF] URL ID not found for hashed URL: " + hashedUrl);
            return docTf;
        }
//        log.info("[calculateDocumentTF] Processing URL ID: " + urlID);

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Double>> tfFutures = new ArrayList<>();

        // 1. parallize get row and norm factor calc
        Map<String, Double> termFrequencies = new ConcurrentHashMap<>();
        for (String queryTerm : queryTokens) {
            tfFutures.add(executor.submit(() -> {
                Row row = KVS.getRow(INDEX_TABLE, queryTerm);
                if (row == null) return 0.0;

                String cellValue = row.get(urlID);
                if (cellValue == null) return 0.0;

                String[] parts = cellValue.split(":");
                if (parts.length < 2) return 0.0;

                String[] freqParts = parts[1].split("/");
                if (freqParts.length < 2) return 0.0;

                double frequency = Double.parseDouble(freqParts[0]);
                termFrequencies.put(queryTerm, frequency);
                return frequency * frequency;
            }));
        }

        double tfSquareSum = 0.0;
        for (Future<Double> future : tfFutures) {
            try {
                tfSquareSum += future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();

        // 2. calc norm factor
        double normalizationFactor = Math.sqrt(tfSquareSum);
//        log.info("[calculateDocumentTF] Normalization factor for URL ID: " + urlID + " is: " + normalizationFactor);

        // 3. parallel norm
        ExecutorService normalizationExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Void>> normalizationFutures = new ArrayList<>();

        for (Map.Entry<String, Double> entry : termFrequencies.entrySet()) {
            normalizationFutures.add(normalizationExecutor.submit(() -> {
                String queryTerm = entry.getKey();
                double rawFrequency = entry.getValue();
                double normalizedTf = rawFrequency / normalizationFactor;
                docTf.put(queryTerm, normalizedTf);
                return null;
            }));
        }

        for (Future<Void> future : normalizationFutures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        normalizationExecutor.shutdown();
        return docTf;
    }


    public double calculateTFIDF(Map<String, Double> queryTF, Map<String, Double> docTf){
        // [Prof approach] - final tf-idf is simply the dot product of query and doc tfidf
        double tfidfScore= 0.0;

        for (Map.Entry<String, Double> entry : queryTF.entrySet()) {
            String term = entry.getKey();
            double queryWeight = entry.getValue();

//            log.info("[calculateTFIDF] term: " + term + " doc tf is: " + queryWeight);
            if (docTf.containsKey(term)) {
                double docWeight = docTf.get(term);
                tfidfScore += queryWeight * docWeight;
            }
        }

//        log.info("[calculateTFIDF] tfidfScore: " + tfidfScore );
        return tfidfScore;
    }
    public Double calculateTitleAndOGMatchScore(String hashedUrl, String query) throws IOException {
        // helper - combine title and og desp weight calculation

        Row row = getCachedRow(PROCESSED_TABLE, hashedUrl);
//        Row row=KVS.getRow(PROCESSED_TABLE, hashedUrl);
        if (row == null) return 0.0;

        String title = row.get("title");
        String ogDescription = row.get("description");

        if ((title == null || title.isEmpty()) && (ogDescription == null || ogDescription.isEmpty())) return 0.0;

        Set<String> queryTokens = new HashSet<>(Arrays.asList(query.toLowerCase().split("\\s+")));

        Set<String> combinedTokens = new HashSet<>();
        if (title != null && !title.isEmpty()) {
            combinedTokens.addAll(Arrays.asList(title.toLowerCase().split("\\s+")));
        }
        if (ogDescription != null && !ogDescription.isEmpty()) {
            combinedTokens.addAll(Arrays.asList(ogDescription.toLowerCase().split("\\s+")));
        }

        // match count
        long matchCount = queryTokens.stream()
                .filter(combinedTokens::contains)
                .count();

        return (double) matchCount / combinedTokens.size();
    }

//    public static boolean isPhraseMatched(List<Integer> positions) {
//        // helper for exact phrase match - can be tuned for approximate phrase search
//        if (positions == null || positions.isEmpty()) {
//            return false;
//        }
//
//        for (int i = 1; i < positions.size(); i++) {
//            if (positions.get(i) != positions.get(i - 1) + 1) {
//                return false;
//            }
//        }
//        return true;
//    }

    public double calculatePhraseMatchScore(List<String> words, Map<String, List<Integer>> positions) {
        //! approximate phrase search
//        List<Integer> bestPositions = getBestPositionWithSorted(words, positions, words.size() + 4, 20);
//        return 1.0 / (Collections.min(bestPositions) + 1);
        return 0.0;
    }


    public Map<String, String> getPageDetails (String hashedUrl) throws IOException {
        // helper to get title, host, icon, url, context for the FE

        Row row = getCachedRow(PROCESSED_TABLE, hashedUrl);
//        Row row=KVS.getRow(PROCESSED_TABLE, hashedUrl);
        log.info("[search] getPageDetails for: " + hashedUrl + ", row: " + row);

        if (row == null) {
            return Map.of("pageContent", "", "icon", "", "url", "", "title", "");
        }

        return Map.of(
                "pageContent", row.get("text") != null ? row.get("text") : "",
                "icon", row.get("icon") != null ? row.get("icon") : "",
                "url", row.get("url") != null ? row.get("url") : "",
                "title", row.get("title") != null ? CapitalizeTitle.toTitleCase(row.get("title")) : ""
        );
    }

    public String extractHostName(String urlString) {
        try {
            URL url = new URL(urlString);
            String host = url.getHost();
            return host.startsWith("www.") ? CapitalizeTitle.toTitleCase(host.substring(4))
                    : CapitalizeTitle.toTitleCase(host);
        } catch (Exception e) {
            return "Unknown";
        }
    }

    public List<String> getAutocompleteSuggestions(String inputWords, int limit) {
        return trie.getWordsWithPrefix(inputWords,10);
    }


    public String getSnapshot(String hashedUrl) throws IOException {
//        String hashedUrl = Hasher.hash(url);
//        Row row = KVS.getRow(PropertyLoader.getProperty("table.craw"), hashedUrl);
//        if (row == null) {
//            log.warn("[search] No row found for URL: " + hashedUrl);
//            return null;
//        }

        byte[] b = KVS.get(PropertyLoader.getProperty("table.processed"), hashedUrl, PropertyLoader.getProperty("table.processed.text"));
        if(b == null){
            return "";
        }
        return new String(b);
    }

    public List<String> getImages(String keyword) throws IOException {
        Row row = KVS.getRow(INDEX_IMAGE_TABLE, keyword);
        if (row == null || row.get("images") == null || row.get("images").isEmpty()) {
            log.warn("[search] No images found for keyword: " + keyword);
            return Collections.emptyList();
        }

        String imagesColumn = row.get("images");
        return Arrays.asList(imagesColumn.split(","));
    }


/*
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

    public String getPageContent(String hashedUrl) throws IOException {
        Row row = KVS.getRow(PROCESSED_TABLE, hashedUrl);
        log.info("[search] getPageContent: " + hashedUrl +  "row: " + row);

        if (row == null) {
            return "";
        }else{
            return row.get("text");
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
<<<<<<< Updated upstream

    public List<String> getAutocompleteSuggestions(String inputWords, int limit) {
        if(inputWords == null || inputWords.isEmpty()){
            return Collections.emptyList();
        }

        String[] words = inputWords.split("-+");
        String lastWord = words[words.length - 1];

        return trie.getWordsWithPrefix(lastWord,10);
    }


    public String getSnapshot(String hashedUrl) throws IOException {
//        String hashedUrl = Hasher.hash(url);
//        Row row = KVS.getRow(PropertyLoader.getProperty("table.craw"), hashedUrl);
//        if (row == null) {
//            log.warn("[search] No row found for URL: " + hashedUrl);
//            return null;
//        }

        byte[] b = KVS.get(PropertyLoader.getProperty("table.processed"), hashedUrl, PropertyLoader.getProperty("table.processed.text"));
        if(b == null){
            return "";
        }
        return new String(b);
    }

    public List<String> getImages(String keyword) throws IOException {
        Row row = KVS.getRow(INDEX_IMAGE_TABLE, keyword);
        if (row == null || row.get("images") == null || row.get("images").isEmpty()) {
            log.warn("[search] No images found for keyword: " + keyword);
            return Collections.emptyList();
        }

        String imagesColumn = row.get("images");
        return Arrays.asList(imagesColumn.split(","));
    }
=======
*/
>>>>>>> Stashed changes
}
