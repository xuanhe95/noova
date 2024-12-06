package org.noova.gateway.service;

//import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.checkerframework.checker.units.qual.A;
import org.noova.gateway.storage.StorageStrategy;
import org.noova.gateway.trie.CacheManager;
import org.noova.gateway.trie.DistanceTrie;
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

    private static DistanceTrie trie;

    private static SearchService instance = null;

    private static final StorageStrategy storageStrategy = StorageStrategy.getInstance();

    private static final TrieManager trieManager = TrieManager.getInstance();

    private static final boolean ENABLE_TRIE_CACHE = PropertyLoader.getProperty("cache.trie.enable").equals("true");

    private static final String PGRK_TABLE = PropertyLoader.getProperty("table.pagerank");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String INDEX_IMAGE_TABLE = PropertyLoader.getProperty("table.image");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String ID_URL_TABLE = PropertyLoader.getProperty("table.id-url");
//    private static final Map<String, String> ID_URL_CACHE = new ConcurrentHashMap<>();

    private static final KVS KVS = storageStrategy.getKVS();

    private static final int totalDocuments = 8692; //! TBD, hardcoded for now

//    private static final Map<String, Double> PAGE_RANK_CACHE = new ConcurrentHashMap<>();
    private final Map<String, Row> processedTableCache = new ConcurrentHashMap<>(); // recent cache

    public static Map<String, String> URL_TO_ID_CACHE = new HashMap<>();
    public static Map<String, String> ID_TO_URL_CACHE = new HashMap<>();
    private static final int MAX_DOMAIN = 2; // limit # of domains in top 200 pgrk page



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
                    trie = (DistanceTrie) trieManager.loadTrie(trieName);
                } else{
                    log.info("[search] Trie not found, building trie...");
                    trie = (DistanceTrie) trieManager.buildTrie(PropertyLoader.getProperty("table.index"));
                    trieManager.saveTrie(trie, trieName);
                }

            } catch (IOException e) {
                log.error("[search] Error loading trie");
            }
        }

        // preload caches
//        ExecutorService executor = Executors.newFixedThreadPool(2);
////        executor.submit(this::preloadIDUrlCache);
//        executor.submit(this::preloadPageRankCache);
//        executor.shutdown();

        //loadWordToUrlsCache();
        loadUrlToIdCache();
        loadIdToUrlCache();
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

//    public  void preloadIDUrlCache() {
//        Iterator<Row> rows = null;
//        try {
//            rows = KVS.scan(ID_URL_TABLE, null, null);
//            while (rows.hasNext()) {
//                Row row = rows.next();
//                String fromUrlId = row.key();
//                String hashedUrl = row.get("value");
//                if (fromUrlId != null && hashedUrl != null) {
//                    ID_URL_CACHE.put(fromUrlId, hashedUrl);
//                }
//            }
//            log.info("[cache] Preloaded ID_URL_TABLE with " + ID_URL_CACHE.size() + " entries.");
//        } catch (IOException e) {
//            log.error("[cache] Error preloading ID_URL_TABLE", e);
//            throw new RuntimeException(e);
//        }
//    }

//    public void preloadPageRankCache() {
//        try {
//            Iterator<Row> rows = KVS.scan(PGRK_TABLE, null, null);
//            while (rows.hasNext()) {
//                Row row = rows.next();
//                String url = row.key();
//                String rankStr = row.get("rank");
//                if (url != null && rankStr != null) {
//                    PAGE_RANK_CACHE.put(url, Double.parseDouble(rankStr));
//                }
//            }
//            log.info("[cache] Preloaded PAGERANK_TABLE with " + PAGE_RANK_CACHE.size() + " entries.");
//        } catch (IOException e) {
//            log.error("[cache] Error preloading PGRK_TABLE", e);
//            throw new RuntimeException(e);
//        }
//    }

    public Row getCachedRow(String tableName, String key) {
        return processedTableCache.computeIfAbsent(key, k -> {
            try {
                return KVS.getRow(tableName, k);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public String getLinkFromID(String urlId) throws IOException {
        System.out.println("here in getLinkFromID");
        String hashedUrl = "";
//        if(ID_URL_CACHE.containsKey(urlId)){
//            hashedUrl = ID_URL_CACHE.get(urlId);
//            System.out.println("found in cache: "+hashedUrl);
//        }
        if(ID_TO_URL_CACHE.containsKey(urlId)){
            hashedUrl = ID_TO_URL_CACHE.get(urlId);
            System.out.println("found in cache: "+hashedUrl);
        } else{
            byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, urlId, "value");
            if(hashedUrlByte != null){
                hashedUrl = new String(hashedUrlByte);
            }
            System.out.println("found in id table"+hashedUrl);

        }
        String url = "";
        if (hashedUrl != null && !hashedUrl.isEmpty()) {
            System.out.println("query processed table"+hashedUrl);
            Row row = getCachedRow(PROCESSED_TABLE, hashedUrl);

            if (row == null) {
                byte[] urlByte = KVS.get(PROCESSED_TABLE,hashedUrl,"url");
                if(urlByte != null){
                    url = new String(urlByte);
                }

            }else{
                url = row.get("url");
            }
            System.out.println("found in process table"+url);
            return url;
        }
        return "";
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

    private List<Integer> parseUrlWithPositionsList(String urlsWithPositions) {
        List<Integer> result = new ArrayList<>();
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
//        List<Integer> result = new ArrayList<>();
//        String rawPosition = urlsWithPositions.split(":")[0];
////        System.out.println(rawPosition);
//        String[] positions = rawPosition.split("\\s+");
////        System.out.println(positions.length);
//        for (String position : positions) {
//            position = Parser.extractNumber(position);
//            assert position != null;
//            if (!position.isEmpty()) {
//                result.add(Integer.parseInt(position));
//            }
//        }
//        return result;

        List<Integer> result = new ArrayList<>();

        String[] positions = urlsWithPositions.split(":")[0].trim().split("\\s+");
        for (String position : positions) {
            // 提取数字后直接检查并转换
            if (position != null && !position.isEmpty()) {
                try {
                    result.add(Integer.parseInt(position.replaceAll("[^0-9]", "")));
                } catch (NumberFormatException e) {
                    // 如果解析失败，跳过该位置
                    System.err.println("Failed to parse position: " + position);
                }
            }
        }
        return result;
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();



    private static Map<String, List<String>> WORD_TO_URLS_CACHE = new HashMap<>();
    private static void loadWordToUrlsCache(){


        try {
            WORD_TO_URLS_CACHE = CacheManager.getInstance().getCache("wordToUrlCache") == null ? new HashMap<>() :
                    (Map<String, List<String>>) CacheManager.getInstance().getCache("wordToUrlCache");



            if(WORD_TO_URLS_CACHE.isEmpty()){
                System.out.println("Loading to cache");
                KVS.scan(INDEX_TABLE, null, null).forEachRemaining(row -> {
                    String word = row.key();
//                    Set<String> urls = new HashSet<>(row.columns());
                    WORD_TO_URLS_CACHE.put(word, new ArrayList<>(row.columns()));
                });
                CacheManager.getInstance().saveCache("wordToUrlCache", WORD_TO_URLS_CACHE);
            }


            for(String word : WORD_TO_URLS_CACHE.keySet()){
                System.out.println("Word: " + word + ", urls: " + WORD_TO_URLS_CACHE.get(word).size());
            }
            System.out.println("Loaded to cache");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadUrlToIdCache() {
        try {
            // Load existing cache or initialize it
            URL_TO_ID_CACHE = CacheManager.getInstance().getCache("urlToIdCache") == null ? new HashMap<>() :
                    (Map<String, String>) CacheManager.getInstance().getCache("urlToIdCache");

            if (URL_TO_ID_CACHE.isEmpty()) {
                System.out.println("Loading URL to ID cache");
                KVS.scan("pt-urltoid", null, null).forEachRemaining(row -> {
                    String url = row.key();
                    String id = row.get("value");
                    if (url != null && id != null) {
                        URL_TO_ID_CACHE.put(url, id);
                    }
                });
                CacheManager.getInstance().saveCache("urlToIdCache", URL_TO_ID_CACHE);
            }

            System.out.println("Loaded URL to ID cache with size: " + URL_TO_ID_CACHE.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadIdToUrlCache() {
        try {
            // Load existing cache or initialize it
            ID_TO_URL_CACHE = CacheManager.getInstance().getCache("idToUrlCache") == null ? new HashMap<>() :
                    (Map<String, String>) CacheManager.getInstance().getCache("idToUrlCache");

            if (ID_TO_URL_CACHE.isEmpty()) {
                System.out.println("Loading ID to URL cache");
                KVS.scan("pt-idtourl", null, null).forEachRemaining(row -> {
                    String id = row.key();
                    String url = row.get("value");
                    if (id != null && url != null) {
                        ID_TO_URL_CACHE.put(id, url);
                    }
                });
                CacheManager.getInstance().saveCache("idToUrlCache", ID_TO_URL_CACHE);
            }

            System.out.println("Loaded ID to URL cache with size: " + ID_TO_URL_CACHE.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


//    public Map<String, Map<String, List<Integer>>> searchByKeywordsIntersection(List<String> keywords) throws IOException {}


    public Map<String, Map<String, List<Integer>>> getHashedUrlPositions(List<String> hashedUrls, int pageLimit){
        return hashedUrls.parallelStream()
                .map(url -> {
                    try {
                        Map<String, List<Integer>> positions = searchByKeywordV2(url);
                        if (!positions.isEmpty()) {
                            return Map.entry(url, positions);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }


//    public Map<String, List<Integer>> getHashedUrlPositions(String hashedUrl, int pageLimit) {
//        Map<String, List<Integer>> result = new HashMap<>();
//        try {
//            Map<String, List<Integer>> positions = searchByKeywordV2(hashedUrl);
//            if (!positions.isEmpty()) {
//                result.put(hashedUrl, positions);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }







    public Map<String, Map<String, List<Integer>>> searchByKeywordsIntersection(List<String> keywords, int pageLimit) throws IOException {
        // uses HASHED_URL as key
        if (keywords == null || keywords.isEmpty()) {
            log.warn("[search] Empty keyword or too less");
            return new HashMap<>();
        }


        Map<String, Map<String, List<Integer>>> allResults = new HashMap<>();

        Map<String, Row> cache = new HashMap<>();

        Set<String> allFromIds = null;


        if(!WORD_TO_URLS_CACHE.isEmpty()){
            System.out.println("Using cache");
            for(String keyword : keywords){
                if(WORD_TO_URLS_CACHE.containsKey(keyword)){
                    System.out.println("Found in cache: " + keyword);
                    Set<String> urls = new HashSet<>( WORD_TO_URLS_CACHE.get(keyword) );
                    if(allFromIds == null){
                        allFromIds = urls;
                    } else{
                        allFromIds.retainAll(urls);
                    }
                }
            }


        } else{
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
        }

        if (allFromIds == null || allFromIds.isEmpty()) {
            log.warn("[search] No row found for keyword: " + keywords);
            return allResults;
        }
        for (String fromUrlId : allFromIds) {

            Map<String, List<Integer>> result = new HashMap<>();

            for (String keyword : keywords) {

                String rawPositions = "";

                if(!WORD_TO_URLS_CACHE.isEmpty()){

                    byte[] b = KVS.get(INDEX_TABLE, keyword, fromUrlId);

                    rawPositions = b == null ? "" : new String(b);

                } else{
                    Row row = cache.getOrDefault(keyword, null);

                    if(row == null){
                        continue;
                    }

                    rawPositions = row.get(fromUrlId);
                }

                if (rawPositions.isEmpty()) {
                    continue;
                }
                List<Integer> positions = parseUrlWithSortedPositions(rawPositions);
                result.put(keyword, positions);
            }


            String hashedUrl = null;


//            if(ID_URL_CACHE.containsKey(fromUrlId)){
//                hashedUrl = ID_URL_CACHE.get(fromUrlId);
//            }

            if(ID_TO_URL_CACHE.containsKey(fromUrlId)){
                hashedUrl = ID_TO_URL_CACHE.get(fromUrlId);
            } else{
                byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, fromUrlId, "value");
                if(hashedUrlByte != null){
                    hashedUrl = new String(hashedUrlByte);
                }else{
                    continue;
                }
            }



            allResults.put(hashedUrl, result);

            if(allResults.size() >= pageLimit){
                break;
            }
        }
        return allResults;

    }

    public Map<String, List<Integer>> getKeywordPositions(Map<String, String> idToUrlMap, Map<String, Row> keywordRows, String urlId){
        Map<String, List<Integer>> result = new HashMap<>();

        keywordRows.forEach((word, row) -> {
            String rawPositions = row.get(urlId);
            if (rawPositions != null && !rawPositions.isEmpty()) {
                List<Integer> positions = parseUrlWithSortedPositions(rawPositions);
                result.put(word, positions);
            }
        });
        return result;
    }


    public Map<String, Map<String, List<Integer>>> searchByKeywordsIntersection(Map<String, Row> keywordRows, List<String> keywords, int pageLimit) throws IOException {
        // uses HASHED_URL as key
        if (keywords == null || keywords.isEmpty()) {
            log.warn("[search] Empty keyword or too less");
            return new HashMap<>();
        }


        Map<String, Map<String, List<Integer>>> allResults = new HashMap<>();

        Map<String, Row> cache = new HashMap<>();

        Set<String> allFromIds = null;


        if(!WORD_TO_URLS_CACHE.isEmpty()){
            System.out.println("Using cache");
            for(String keyword : keywords){
                if(WORD_TO_URLS_CACHE.containsKey(keyword)){
                    System.out.println("Found in cache: " + keyword);
                    Set<String> urls = new HashSet<>( WORD_TO_URLS_CACHE.get(keyword) );
                    if(allFromIds == null){
                        allFromIds = urls;
                    } else{
                        allFromIds.retainAll(urls);
                    }
                }
            }


        } else{
            for (String keyword : keywords) {
                Row row = keywordRows.get(keyword);
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
        }

        if (allFromIds == null || allFromIds.isEmpty()) {
            log.warn("[search] No row found for keyword: " + keywords);
            return allResults;
        }
        for (String fromUrlId : allFromIds) {

            Map<String, List<Integer>> result = new HashMap<>();

            for (String keyword : keywords) {

                String rawPositions = "";

                if(!WORD_TO_URLS_CACHE.isEmpty()){

                    byte[] b = KVS.get(INDEX_TABLE, keyword, fromUrlId);

                    rawPositions = b == null ? "" : new String(b);

                } else{
                    Row row = cache.getOrDefault(keyword, null);

                    if(row == null){
                        continue;
                    }

                    rawPositions = row.get(fromUrlId);
                }

                if (rawPositions.isEmpty()) {
                    continue;
                }
                List<Integer> positions = parseUrlWithSortedPositions(rawPositions);
                result.put(keyword, positions);
            }

            String hashedUrl = null;

//            if(ID_URL_CACHE.containsKey(fromUrlId)){
//                hashedUrl = ID_URL_CACHE.get(fromUrlId);
//            }

            if(ID_TO_URL_CACHE.containsKey(fromUrlId)){
                hashedUrl = ID_TO_URL_CACHE.get(fromUrlId);
            }
            else{
                byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, fromUrlId, "value");
                if(hashedUrlByte != null){
                    hashedUrl = new String(hashedUrlByte);
                }else{
                    continue;
                }
            }

            allResults.put(hashedUrl, result);

            if(allResults.size() >= pageLimit){
                break;
            }
        }
        return allResults;

    }

    public Map<String, List<Integer>> searchByKeywordV2(String keyword) throws IOException {
        // uses HASHED_URL as key

        if (keyword == null || keyword.isEmpty()) {
            log.warn("[search] Empty keyword");
            return new HashMap<>();
        }

        Map<String, List<Integer>> result = new HashMap<>();
        Row row = KVS.getRow(INDEX_TABLE, keyword);
        if (row == null) {
            log.warn("[search] No row found for keyword: " + keyword);
            return result;
        }

        Set<String> fromIds = row.columns();

        for (String fromUrlId : fromIds) {
            String texts = row.get(fromUrlId);
            String hashedUrl = null;

            if(ID_TO_URL_CACHE.containsKey(fromUrlId)){
                hashedUrl = ID_TO_URL_CACHE.get(fromUrlId);
            }
            else{
                byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, fromUrlId, "value");
                if(hashedUrlByte != null){
                    hashedUrl = new String(hashedUrlByte);
                }else{
                    continue;
                }
            }

            if(texts.isEmpty()){
                continue;
            }
            List<Integer> positions = parseUrlWithPositionsList(texts);
            if(result.containsKey(hashedUrl)){
                result.get(hashedUrl).addAll(positions);
            } else{
                result.put(hashedUrl, positions);
            }
        }
        return result;
    }

    public Map<String, List<Integer>> searchByKeywordV2(Row keywordRow) throws IOException {
        // uses HASHED_URL as key

        Map<String, List<Integer>> result = new HashMap<>();


        Set<String> fromIds = keywordRow.columns();

        for (String fromUrlId : fromIds) {
            String texts = keywordRow.get(fromUrlId);
            String hashedUrl = null;

            if(ID_TO_URL_CACHE.containsKey(fromUrlId)){
                hashedUrl = ID_TO_URL_CACHE.get(fromUrlId);
            }
            else{
                byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, fromUrlId, "value");
                if(hashedUrlByte != null){
                    hashedUrl = new String(hashedUrlByte);
                }else{
                    continue;
                }
            }

            if(texts.isEmpty()){
                continue;
            }
            List<Integer> positions = parseUrlWithPositionsList(texts);
            if(result.containsKey(hashedUrl)){
                result.get(hashedUrl).addAll(positions);
            } else{
                result.put(hashedUrl, positions);
            }
        }
        return result;
    }


    public Map<String, Map<String, List<Integer>>> searchByKeywordsIntersectionV2(Map<String, String> idToUrlMap, Map<String, Row> keywordRows, List<String> keywords, int pageLimit) throws IOException {
        // urls to words to positions

        if (keywords == null || keywords.isEmpty()) {
            log.warn("[search] Empty keyword or too less");
            return new HashMap<>();
        }


        Map<String, Map<String, List<Integer>>> allResults = new HashMap<>();

        Set<String> allFromIds = idToUrlMap.keySet();

        if (allFromIds == null || allFromIds.isEmpty()) {
            log.warn("[search] No row found for keyword: " + keywords);
            return allResults;
        }
        for (String fromUrlId : allFromIds) {
            Map<String, List<Integer>> result = new HashMap<>();

            for (String keyword : keywordRows.keySet()) {

                Row keywordRow = keywordRows.get(keyword);
                String rawPositions = keywordRow.get(fromUrlId);
                List<Integer> positions = parseUrlWithSortedPositions(rawPositions);

                System.out.println("positions: "+positions.size());
                result.put(keyword, positions);
            }
            allResults.put(idToUrlMap.get(fromUrlId), result);

            if(allResults.size() >= pageLimit){
                break;
            }
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


//            byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, fromUrlId, "value");

            String hashedUrl = null;

//            if(hashedUrlByte != null){
//                hashedUrl = new String(hashedUrlByte);
//            } else{
//                continue;
//            }

            if(ID_TO_URL_CACHE.containsKey(fromUrlId)){
                hashedUrl = ID_TO_URL_CACHE.get(fromUrlId);
            }
            else{
                byte[] hashedUrlByte = KVS.get(ID_URL_TABLE, fromUrlId, "value");
                if(hashedUrlByte != null){
                    hashedUrl = new String(hashedUrlByte);
                }else{
                    continue;
                }
            }


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

//    public SortedMap<Double, String> sortByPageRank(Map<String, List<Integer>> urlsWithPositions) {
//
//        SortedMap<Double, String> result = new TreeMap<>(Comparator.reverseOrder());
//        urlsWithPositions.forEach((normalizedUrl, positions) -> {
//            try {
//                double pagerank = getPagerank(normalizedUrl);
//                log.info("[search] Found pagerank: " + pagerank + " for URL: " + normalizedUrl);
//                result.put(pagerank, normalizedUrl);
//            } catch (IOException e) {
//                log.error("[search] Error getting pagerank for URL: " + normalizedUrl);
//            }
//        });
//
//        return result;
//    }

    public double getPagerank(String hashedUrl) throws IOException {

//        // get the row from the pagerank table
//        Row row = KVS.getRow(PGRK_TABLE, hashedUrl);
//        if (row == null) {
//            log.warn("[search] No row found for URL: " + hashedUrl);
//            return 0.0;
//        }
//        log.info("[search] Found row: " + hashedUrl);
//        String rank = row.get("rank");
//        if (rank == null) {
//            log.warn("[search] No rank found for URL: " + hashedUrl);
//            return 0.0;
//        }

        byte[] rankByte = KVS.get(PGRK_TABLE, hashedUrl, "rank");
        if(rankByte == null){
            return 0.0;
        }

        return Double.parseDouble(new String(rankByte));

//        return PAGE_RANK_CACHE.getOrDefault(hashedUrl, 0.0);
    }


    public String extractRootUrl(String url) {
        try {
            URL parsedUrl = new URL(url);
            String path = parsedUrl.getPath();
            // rm query param
            if (path.contains("?")) {
                path = path.substring(0, path.indexOf('?'));
            }
            return parsedUrl.getProtocol() + "://" + parsedUrl.getHost() + path;
        } catch (Exception e) {
            return url; // fallback to original url
        }
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


    public SortedMap<String, Double> getPageRanksParallel(Set<String> hashedUrls, int limit) throws IOException {
        Map<String, Double> result = new HashMap<>();


        System.out.println("hashedUrls size: "+hashedUrls.size());
        ExecutorService executor = Executors.newFixedThreadPool(10); // 最大并发请求数为 2
        try {
            result = hashedUrls.parallelStream()
                    .map(hashedUrl -> {
                        try {
                            byte[] rankByte = KVS.get(PGRK_TABLE, hashedUrl, "rank");
                            if (rankByte != null) {
                                return Map.entry(hashedUrl, Double.parseDouble(new String(rankByte)));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toConcurrentMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (v1, v2) -> v1, // 冲突时保留第一个值
                            ConcurrentHashMap::new
                    ));
        } finally {
            executor.shutdown();
        }


        System.out.println("result size: "+result.size());

        // 对结果进行排序，并限制返回前 `limit` 个
        Map<String, Double> finalResult = result;
        return result.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed()) // 按值降序排序
                .limit(limit) // 限制返回数量
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> v1, // 冲突时保留第一个值（实际上不应该有冲突）
                        () -> new TreeMap<>(Comparator.comparingDouble(finalResult::get).reversed()) // 返回有序的 TreeMap
                ));

        // sort pgrk descending
//        List<Map.Entry<String, Double>> sortedEntries = result.entrySet().stream()
//                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
//                .toList();
//
//        // limit pgrk pages by limit(200) and MAX_DOMAIN(3)
//        Map<String, Integer> domainCounts = new HashMap<>();
//        Map<String, Double> limitedResults = new LinkedHashMap<>();
//        Map<String, Set<String>> rootInDomain = new HashMap<>(); // diff root within same domain
//        int count = 0;
//
//        for (Map.Entry<String, Double> entry : sortedEntries) {
//            if (count >= limit) break;
//
//            String hashedUrl = entry.getKey();
////            String url = KVS.getRow(PROCESSED_TABLE, hashedUrl).get("url");
//            String url = getCachedRow(PROCESSED_TABLE,hashedUrl).get("url");
//            String domain = extractHostName(url);
//            String root = extractRootUrl(url);
//            rootInDomain.computeIfAbsent(domain, k -> new HashSet<>());
//
//            if(rootInDomain.containsKey(domain) && !rootInDomain.get(domain).add(root)){
//                continue; // seen domain+root
//            }
//
//            domainCounts.put(domain, domainCounts.getOrDefault(domain, 0) + 1);
//            if (domainCounts.get(domain) <= MAX_DOMAIN) {
//                limitedResults.put(hashedUrl, entry.getValue());
//                count++;
//            }
//        }
//
//        // sort in treemap
//        SortedMap<String, Double> sortedMap = new TreeMap<>((a, b) -> {
//            int cmp = Double.compare(limitedResults.get(b), limitedResults.get(a));
//            return cmp == 0 ? a.compareTo(b) : cmp;
//        });
//        sortedMap.putAll(limitedResults);
//        return sortedMap;


//        Map<String, Double> result = new HashMap<>();
//
//        for(String hashedUrl : hashedUrls){
//            byte[] rankByte = KVS.get(PGRK_TABLE, hashedUrl, "rank");
//            if(rankByte

    }



    public SortedMap<String, Double> getPageRanks(Set<String> hashedUrls, int limit) throws IOException {
        Map<String, Double> result = new HashMap<>();
        Map<String, List<String>> domainToUrls = new HashMap<>();

        // get pgrk pages
        System.out.println("hashedUrls size: "+hashedUrls.size());


        for(String hashedUrl : hashedUrls){
            byte[] rankByte = KVS.get(PGRK_TABLE, hashedUrl, "rank");
            if (rankByte != null) {
                double pageRank = Double.parseDouble(new String(rankByte));
                result.put(hashedUrl, pageRank);

                String domain = extractHostName(SearchService.ID_TO_URL_CACHE.get(hashedUrl));
                domainToUrls.computeIfAbsent(domain, k -> new ArrayList<>()).add(hashedUrl);
            }
        }

        // sort pgrk descending
        List<Map.Entry<String, Double>> sortedEntries = result.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .toList();

        // limit pgrk pages by limit(200) and MAX_DOMAIN(3)
        Map<String, Integer> domainCounts = new HashMap<>();
        Map<String, Double> limitedResults = new LinkedHashMap<>();
        Map<String, Set<String>> rootInDomain = new HashMap<>(); // diff root within same domain
        int count = 0;

        for (Map.Entry<String, Double> entry : sortedEntries) {
            if (count >= limit) break;

            String hashedUrl = entry.getKey();
//            String url = KVS.getRow(PROCESSED_TABLE, hashedUrl).get("url");
            String url = getCachedRow(PROCESSED_TABLE,hashedUrl).get("url");
            String domain = extractHostName(url);
            String root = extractRootUrl(url);
            rootInDomain.computeIfAbsent(domain, k -> new HashSet<>());

            if(rootInDomain.containsKey(domain) && !rootInDomain.get(domain).add(root)){
                continue; // seen domain+root
            }

            domainCounts.put(domain, domainCounts.getOrDefault(domain, 0) + 1);
            if (domainCounts.get(domain) <= MAX_DOMAIN) {
                limitedResults.put(hashedUrl, entry.getValue());
                count++;
            }
        }

        // sort in treemap
        SortedMap<String, Double> sortedMap = new TreeMap<>((a, b) -> {
            int cmp = Double.compare(limitedResults.get(b), limitedResults.get(a));
            return cmp == 0 ? a.compareTo(b) : cmp;
        });
        sortedMap.putAll(limitedResults);
        return sortedMap;


//        Map<String, Double> result = new HashMap<>();
//
//        for(String hashedUrl : hashedUrls){
//            byte[] rankByte = KVS.get(PGRK_TABLE, hashedUrl, "rank");
//            if(rankByte != null){
//                result.put(hashedUrl, Double.parseDouble(new String(rankByte)));
//            }
//        }
//
//        var sortedEntries = result.entrySet().stream()
//                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) // 倒序排序
//                .limit(limit) // 只保留前 limit 个
//                .toList();
//
//        // 创建一个有序的 TreeMap
//        var limitedMap = new TreeMap<String, Double>((a, b) -> {
//            int cmp = result.get(b).compareTo(result.get(a)); // 按值排序
//            return cmp == 0 ? a.compareTo(b) : cmp; // 如果值相同，按键排序
//        });
//
//        for (var entry : sortedEntries) {
//            limitedMap.put(entry.getKey(), entry.getValue());
//        }
//
//        return limitedMap;
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


    public List<List<String>> getCorrectionWords(List<String> listOfWord, int limit){
        List<List<String>> result = Collections.synchronizedList(new ArrayList<>());

        listOfWord.stream().map(word -> trie.getWordsWithinEditDistancePriority(word, 1, limit)).forEach(result::add);

        return result;
    }
    public List<String> getCorrection(String lastWord,   int limit) {

        return trie.getWordsWithinEditDistancePriority(lastWord, 2, limit);
    }

    static class Node implements Comparable<Node> {
        int value;
        int index;
        int listIndex;

        Node(int value, int index, int listIndex) {
            this.value = value;
            this.index = index;
            this.listIndex = listIndex;
        }

        @Override
        public int compareTo(Node other) {
            return Integer.compare(this.value, other.value);
        }
    }

    public List<Integer> getBestPositionWithSorted(List<String> words, Map<String, List<Integer>> positions, int spanTolerance, int spanLimit) {
        List<List<Integer>> allPositions = new ArrayList<>();
        PriorityQueue<Node> pq = new PriorityQueue<>();

        for (String word : words) {
            if (positions.containsKey(word)) {
                List<Integer> posSortedList = positions.get(word);

                allPositions.add(posSortedList);
                pq.offer(new Node(posSortedList.get(0), 0, allPositions.size() - 1));
            } else {
                return new ArrayList<>();
            }
        }

        int minDistance = Integer.MAX_VALUE;
        int maxPos = Integer.MIN_VALUE;
        List<Integer> result = new ArrayList<>();

        for (List<Integer> posList : allPositions) {
            maxPos = Math.max(maxPos, posList.get(0));
        }

        while (true) {
            Node minNode = pq.poll();
            int minPos = minNode.value;

            int currentDistance = maxPos - minPos;
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                result.clear();
                for (Node node : pq) {
                    result.add(allPositions.get(node.listIndex).get(node.index));
                }
                result.add(minPos);
            }

            if (currentDistance <= spanTolerance) {
                return result;
            }

            List<Integer> currentList = allPositions.get(minNode.listIndex);
            if (minNode.index + 1 < currentList.size()) {
                int nextValue = currentList.get(minNode.index + 1);
                pq.offer(new Node(nextValue, minNode.index + 1, minNode.listIndex));
                maxPos = Math.max(maxPos, nextValue);
            } else {
                break;
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


        for (String word : words) {
            if (positions.containsKey(word)) {
                Set<Integer> posSet = positions.get(word);

                List<Integer> posList = new ArrayList<>(positions.get(word));
                Collections.sort(posList);
                allPositions.add(posList);
                pq.offer(new Node(posList.get(0), 0, allPositions.size() - 1));
            } else {
                return new ArrayList<>();
            }
        }

        int minDistance = Integer.MAX_VALUE;
        int maxPos = Integer.MIN_VALUE;
        List<Integer> result = new ArrayList<>();

        for (List<Integer> posList : allPositions) {
            maxPos = Math.max(maxPos, posList.get(0));
        }

        while (true) {
            Node minNode = pq.poll();
            int minPos = minNode.value;

            int currentDistance = maxPos - minPos;
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                result.clear();
                for (Node node : pq) {
                    result.add(allPositions.get(node.listIndex).get(node.index));
                }
                result.add(minPos);
            }

            if (currentDistance <= spanTolerance) {
                return result;
            }

            List<Integer> currentList = allPositions.get(minNode.listIndex);
            if (minNode.index + 1 < currentList.size()) {
                int nextValue = currentList.get(minNode.index + 1);
                pq.offer(new Node(nextValue, minNode.index + 1, minNode.listIndex));
                maxPos = Math.max(maxPos, nextValue); // 更新最大位置
            } else {
                break;
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

        int globalMinPos = Integer.MAX_VALUE;

        for (String word : words) {
            if (positions.containsKey(word)) {
                List<Integer> posList = new ArrayList<>(positions.get(word));
                Collections.sort(posList);
                allPositions.add(posList);
                pq.offer(new Node(posList.get(0), 0, allPositions.size() - 1));

                globalMinPos = Math.min(globalMinPos, posList.get(0));
            } else {
                return new ArrayList<>();
            }
        }

        int minDistance = Integer.MAX_VALUE;
        int maxPos = globalMinPos;
        List<Integer> result = new ArrayList<>();

        for (List<Integer> posList : allPositions) {
            maxPos = Math.max(maxPos, posList.get(0));
        }

        while (!pq.isEmpty()) {
            Node minNode = pq.poll();
            int minPos = minNode.value;

            // 更新最小距离和结果
            int currentDistance = maxPos - minPos;
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                result.clear();
                for (Node node : pq) {
                    result.add(allPositions.get(node.listIndex).get(node.index));
                }
                result.add(minPos);
            }

            if (currentDistance <= maxSpan) {
                return result;
            }

            List<Integer> currentList = allPositions.get(minNode.listIndex);
            if (minNode.index + 1 < currentList.size()) {
                int nextValue = currentList.get(minNode.index + 1);
                pq.offer(new Node(nextValue, minNode.index + 1, minNode.listIndex));
                maxPos = Math.max(maxPos, nextValue);
            } else {
                break;
            }
        }

        return result;
    }


    public List<Integer> getBestPosition(List<String> words, Map<String, Set<Integer>> positions){
        List<Integer> result = new ArrayList<>();

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

        int[] indices = new int[allPositions.size()];
        int minDistance = Integer.MAX_VALUE;

        while (true) {
            int minPos = Integer.MAX_VALUE;
            int maxPos = Integer.MIN_VALUE;
            int minListIndex = -1;

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

            int currentDistance = maxPos - minPos;
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                result.clear();
                for (int i = 0; i < allPositions.size(); i++) {
                    result.add(allPositions.get(i).get(indices[i]));
                }
            }

            indices[minListIndex]++;
            if (indices[minListIndex] >= allPositions.get(minListIndex).size()) {
                break;
            }
        }

        return result;

    }

    public Map<String, List<Integer>> calculateSortedPosition(List<String> words, Map<String, Map<String, List<Integer>>> results) throws IOException {

//        System.out.println("words: " + words);
//
        Map<String, List<Integer>> result = new HashMap<>();
        Map<String, Integer> urlToSpan = new HashMap<>();
//        if (words == null || words.isEmpty()) {
//            return new HashMap<>();
//        }

//        // url -> word -> positions
//        Map<String, Map<String, List<Integer>>> results = searchByKeywordsIntersection(words);
        if (results == null || results.isEmpty()) {
            return new HashMap<>();
        }

        for (String url : results.keySet()) {
            Map<String, List<Integer>> wordPositionPair = results.get(url);
            if (wordPositionPair == null || wordPositionPair.isEmpty()) {
                continue;
            }

            List<Integer> best = getBestPositionWithSorted(words, wordPositionPair, words.size() + 4, 10);

            if(best.isEmpty()){
                continue;
            }

            int span = Collections.max(best) - Collections.min(best);
            //System.out.println("best: " + best + ", span: " + span);

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

    public Map<String, List<Integer>> calculateSortedPosition(List<String> words) throws IOException {

        System.out.println("words: " + words);

        Map<String, List<Integer>> result = new HashMap<>();
        Map<String, Integer> urlToSpan = new HashMap<>();
        if (words == null || words.isEmpty()) {
            return new HashMap<>();
        }

        // url -> word -> positions
        Map<String, Map<String, List<Integer>>> results = searchByKeywordsIntersection(words, 200);
        if (results == null || results.isEmpty()) {
            return new HashMap<>();
        }

        for (String url : results.keySet()) {
            Map<String, List<Integer>> wordPositionPair = results.get(url);
            if (wordPositionPair == null || wordPositionPair.isEmpty()) {
                continue;
            }

            List<Integer> best = getBestPositionWithSorted(words, wordPositionPair, words.size() + 5, 10);

            if(best.isEmpty()){
                continue;
            }

            int span = Collections.max(best) - Collections.min(best);
            //System.out.println("best: " + best + ", span: " + span);

            result.put(url, best);
            urlToSpan.put(url, span);

            result.put(url, best);
        }

        return urlToSpan.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> result.get(entry.getKey()),
                        (e1, e2) -> e1,
                        LinkedHashMap::new
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
//                .filter(entry -> urlToPositions.containsKey(entry.getKey()))
//                .filter(entry -> urlToPositions.get(entry.getKey()) != null)
//                .filter(entry -> !urlToPositions.get(entry.getKey()).isEmpty())
//                .sorted(Map.Entry.comparingByValue()) // 按跨度排序
//                .collect(Collectors.toMap(
//                        Map.Entry::getKey,
//                        entry -> urlToPositions.get(entry.getKey()),
//                        (e1, e2) -> e1,
//                        LinkedHashMap::new
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

        if (content == null || content.isEmpty()) {
            System.out.println("Empty content or positions");
            return "";
        }
        String[] words = content.split("\\s+");

        if(positions == null || positions.isEmpty()){
            //return String.join(" ", Arrays.copyOfRange(words, 0, Math.min(words.length, wordLimit)));
            return "";
        }

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


    public Map<String, Double> calculateQueryTF(Map<String, Row> keywordRows) throws IOException {
        // [Prof approach] - apply idf to query, NOT to both query and doc; query does not need normalization
        Map<String, Double> queryTf = new HashMap<>(); // term:tf

        keywordRows.forEach((term, row) -> {
            if (row == null) {
                queryTf.put(term, 0.0);
            } else {
                int df = row.columns().size();
                if (df == 0) {
                    queryTf.put(term, 0.0);
                } else {
                    double idf = Math.log10((double) totalDocuments / df); // inverse N/n_i
                    double tf = Collections.frequency(keywordRows.keySet(), term); // freq of a word in query, no need norm
                    queryTf.put(term, tf * idf);
                }
            }
        });


        return queryTf;
    }


    public Map<String, Double> calculateDocumentTF(String hashedUrl, String query) throws IOException {
        // [Prof approach] - pick a doc to score, normalize tf in a doc freq/sqrt(w_i^2)

        Map<String, Double> docTf = new HashMap<>(); // doc_term:norm_tf
        List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));

        // convert hashurl to id
//        String urlID = KVS.getRow("pt-urltoid", hashedUrl).get("value");
        String urlID = URL_TO_ID_CACHE.get(hashedUrl);

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



    public Map<String, Double> calculateDocumentTF(String hashedUrl, List<String> queryTokens) throws IOException {
        // [Prof approach] - pick a doc to score, normalize tf in a doc freq/sqrt(w_i^2)

        Map<String, Double> docTf = new HashMap<>(); // doc_term:norm_tf

        // convert hashurl to id
//        byte[] urlIdBytes = KVS.get("pt-urltoid", hashedUrl, "value");
//
//        if (urlIdBytes == null) {
////            log.error("[calculateDocumentTF] URL ID not found for hashed URL: " + hashedUrl);
//            return docTf;
//        }
//        String urlID = new String(urlIdBytes);

        String urlID = URL_TO_ID_CACHE.get(hashedUrl);

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



    public Map<String, Double> calculateDocumentTF(String hashedUrl, Map<String, Row> keywordRows) throws IOException {
        // [Prof approach] - pick a doc to score, normalize tf in a doc freq/sqrt(w_i^2)

        Map<String, Double> docTf = new HashMap<>(); // doc_term:norm_tf

        // convert hashurl to id
//        byte[] urlIdBytes = KVS.get("pt-urltoid", hashedUrl, "value");
//
//        if (urlIdBytes == null) {
////            log.error("[calculateDocumentTF] URL ID not found for hashed URL: " + hashedUrl);
//            return docTf;
//        }
//        String urlID = new String(urlIdBytes);

        String urlID = URL_TO_ID_CACHE.get(hashedUrl);

//        log.info("[calculateDocumentTF] Processing URL ID: " + urlID);


        // iterate keyword rows to get precomputed doc tf (indexer schema: position:doctf)
        for(Map.Entry<String, Row> entry : keywordRows.entrySet()){
            String queryTerm = entry.getKey();
            Row row = entry.getValue();
            if (row == null) {
                continue;
            }

            String cellValue = row.get(urlID);
            if (cellValue == null) {
                continue;
            }
//            System.out.println("cellValue: "+cellValue);

            String[] parts = cellValue.split(":");
            if (parts.length < 2) {
                continue;
            }
//            System.out.println("Double.parseDouble(parts[1]): "+Double.parseDouble(parts[1]));
            docTf.put(queryTerm, Double.parseDouble(parts[1]));
        }


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


    public Double calculateTitleAndOGMatchScore(Row pageRow, List<String> words) throws IOException {
        // helper - combine title and og desp weight calculation

//        Row row=KVS.getRow(PROCESSED_TABLE, hashedUrl);
        if (pageRow == null) return 0.0;

        String title = pageRow.get("title");
        String ogDescription = pageRow.get("description");

        if ((title == null || title.isEmpty()) && (ogDescription == null || ogDescription.isEmpty())) return 0.0;

        Set<String> queryTokens = new HashSet<>(words);

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

//    public double calculatePhraseMatchScore(List<String> queryTokens, Map<String, Map<String, List<Integer>>> wordToUrlToPositions, String firstToken) {
//        // url -> positions
//
//
//        //! approximate phrase search
//        List<Integer> bestPositions = getBestPositionWithSorted(queryTokens, , wordToUrlToPositions.keySet().size() + 20, 20);
//        if (bestPositions == null || bestPositions.isEmpty() || bestPositions.size() < wordToUrlToPositions.keySet().size()) {
//            return 0.0;
//        }
//        int span = bestPositions.get(0)- bestPositions.get(bestPositions.size() - 1)  + 1;
//        int inversions = countInversions(bestPositions);
//        double spanWeight = 0.7; // Weight for span
//        double orderWeight = 0.3; // Weight for sequential order
//        System.out.println("(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)): "+(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)));
//        return (spanWeight / (span + 1)) + (orderWeight / (inversions + 1));
////        return 0.0;
//    }

    public double calculatePhraseMatchScore(List<String> words, Map<String, List<Integer>> positions) {
        // url -> positions
        if(positions == null || positions.isEmpty()){
            return 0.0;
        }

        //! approximate phrase search
        List<Integer> bestPositions = getBestPositionWithSorted(words, positions, words.size() + 10, 20);
        if (bestPositions == null || bestPositions.isEmpty() || bestPositions.size() < words.size()) {
            return 0.0;
        }
        int span = bestPositions.get(0)- bestPositions.get(bestPositions.size() - 1)  + 1;
        int inversions = countInversions(bestPositions);
        double spanWeight = 0.7; // Weight for span
        double orderWeight = 0.3; // Weight for sequential order
        System.out.println("(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)): "+(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)));
        return (spanWeight / (span + 1)) + (orderWeight / (inversions + 1));
//        return 0.0;
    }

    public Map<List<Integer>, Double> calculatePhraseMatchScoreV2(List<String> words, Map<String, List<Integer>> positions) {
        // url -> positions
        if(positions == null || positions.isEmpty()){
            return new HashMap<>();
        }

        //! approximate phrase search
        List<Integer> bestPositions = getBestPositionWithSorted(words, positions, words.size() + 10, 20);
        if (bestPositions == null || bestPositions.isEmpty() || bestPositions.size() < words.size()) {
            return new HashMap<>();
        }
        int span = bestPositions.get(0)- bestPositions.get(bestPositions.size() - 1)  + 1;
        int inversions = countInversions(bestPositions);
        double spanWeight = 0.7; // Weight for span
        double orderWeight = 0.3; // Weight for sequential order
        System.out.println("(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)): "+(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)));
        double score=  (spanWeight / (span + 1)) + (orderWeight / (inversions + 1));

        Map<List<Integer>, Double> result = new HashMap<>();
        result.put(bestPositions, score);
        return result;

//        return 0.0;
    }


    public double calculatePhraseMatchScore(List<Integer> bestPosition) {
        //! approximate phrase search

        int span = bestPosition.get(0)- bestPosition.get(bestPosition.size() - 1)  + 1;
        int inversions = countInversions(bestPosition);
        double spanWeight = 0.7; // Weight for span
        double orderWeight = 0.3; // Weight for sequential order
        System.out.println("(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)): "+(spanWeight / (span + 1)) + (orderWeight / (inversions + 1)));
        return (spanWeight / (span + 1)) + (orderWeight / (inversions + 1));
//        return 0.0;
    }

    // Helper method to count inversions (out-of-order positions)
    private int countInversions(List<Integer> positions) {
        int inversions = 0;
        for (int i = 0; i < positions.size(); i++) {
            for (int j = i + 1; j < positions.size(); j++) {
                if (positions.get(i) > positions.get(j)) {
                    inversions++;
                }
            }
        }
        return inversions;
    }


    public String getPageContent(String hashedUrl) throws IOException {
        byte[] b = KVS.get(PropertyLoader.getProperty("table.processed"), hashedUrl, PropertyLoader.getProperty("table.processed.text"));
        if(b == null){
            System.out.println("No content found for URL: " + hashedUrl);
        }
        return b == null ? "" : new String(b);
    }

    public Map<String, String> getPageDetails (String hashedUrl) throws IOException {
        // helper to give host, icon, url, context for the FE

        Row row = getCachedRow(PROCESSED_TABLE, hashedUrl);
//        Row row = KVS.getRow(PROCESSED_TABLE, hashedUrl);
        log.info("[search] getPageDetails for: " + hashedUrl + ", row: " + row);

        Map<String, String> details = new HashMap<>();
        if (row != null) {
            details.put("pageContent", row.get("text") != null ? row.get("text") : "");
            details.put("icon", row.get("icon") != null ? row.get("icon") : "");
            details.put("url", row.get("url") != null ? row.get("url") : "");
            details.put("title", row.get("title")!= null ? CapitalizeTitle.toTitleCase(row.get("title")) : "");
        } else {
            details.put("pageContent", "");
            details.put("icon", "");
            details.put("url", "");
            details.put("title","");
        }

        return details;
    }
//    public Map<String, String> getPageDetails (String hashedUrl) throws IOException {
//        // helper to get title, host, icon, url, context for the FE
//
//        Row row = getCachedRow(PROCESSED_TABLE, hashedUrl);
////        Row row=KVS.getRow(PROCESSED_TABLE, hashedUrl);
//        log.info("[search] getPageDetails for: " + hashedUrl + ", row: " + row);
//
//        if (row == null) {
//            return Map.of("pageContent", "", "icon", "", "url", "", "title", "");
//        }
//
//        return Map.of(
//                "pageContent", row.get("text") != null ? row.get("text") : "",
//                "icon", row.get("icon") != null ? row.get("icon") : "",
//                "url", row.get("url") != null ? row.get("url") : "",
//                "title", row.get("title") != null ? CapitalizeTitle.toTitleCase(row.get("title")) : ""
//        );
//    }


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
*/

}
