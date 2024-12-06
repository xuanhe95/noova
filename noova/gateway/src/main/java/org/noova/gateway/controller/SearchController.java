package org.noova.gateway.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.openjson.JSONException;
import com.github.openjson.JSONObject;
import org.noova.gateway.service.SearchService;
import org.noova.gateway.service.Service;
import org.noova.gateway.service.WeatherService;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.Parser;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


/**
 * @author Xuanhe Zhang
 */
public class SearchController implements IController {

    private static final Logger log = Logger.getLogger(SearchController.class);

    private static SearchController instance;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final SearchService SEARCH_SERVICE = SearchService.getInstance();

    private static final double tfIDFWeight = 0.4; // Need more analysis
    private static final double pgrkWeight = 0.1;
    private static final double titleDespMatchWeight = 0.2;
    private static final double phraseMatchWeight = 0.3;

    private static final int context_view = 30; // Can be in the config file

    private SearchController() {
    }

    public static SearchController getInstance() {
        if (instance == null) {
            instance = new SearchController();
        }
        return instance;
    }

    @Route(path = "/search/key", method = "GET")
    private void searchByKeyword(Request req, Response res) throws IOException {
        log.info("[search] Searching by keyword");
        String keyword = req.queryParams("keyword");
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));

        log.info("[search] Searching by keyword: " + keyword);
        Map<String, Set<Integer>> urlsWithPositions = SEARCH_SERVICE.searchByKeyword(keyword);
        urlsWithPositions.forEach((normalizedUrl, position) -> {
            log.info("[search] Found keyword: " + keyword + " at " + normalizedUrl + ": " + position);
        });

        // Paginate results using the extracted method
        Map<String, Set<Integer>> paginatedResult = Parser.paginateResults(urlsWithPositions, limit, offset);

        String json = OBJECT_MAPPER.writeValueAsString(paginatedResult );
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/search/pagerank", method = "GET")
    private void searchByPageRank(Request req, Response res) throws IOException {
        log.info("[search] Searching by page rank");
        String keyword = req.queryParams("keyword");
        log.info("[search] Searching by keyword: " + keyword);
        Map<String, Set<Integer>> urlsWithPositions = SEARCH_SERVICE.searchByKeyword(keyword);
        SortedMap<Double, String> sortedUrls = SearchService.getInstance().sortByPageRank(urlsWithPositions);

        String json = OBJECT_MAPPER.writeValueAsString(sortedUrls);
        res.body(json);
        res.type("application/json");
    }


    @Route(path = "/search/predict", method = "GET")
    private void searchByKeywordPredict(Request req, Response res) throws IOException {
        log.info("[search] Predicting by keyword");
        String keyword = req.queryParams("keyword");
        String limit = req.queryParams("limit") == null ? "10" : req.queryParams("limit");
        List<String> urls = SEARCH_SERVICE.predict(keyword, Integer.parseInt(limit));
        res.body(urls.toString());
    }

    @Route(path = "/search/predict/word", method = "GET")
    private void searchByWordPredict(Request req, Response res) throws IOException {
        log.info("[search] Predicting by word");
        String keyword = req.queryParams("keyword");
        String limit = req.queryParams("limit") == null ? "10" : req.queryParams("limit");
        //List<String> urls = SearchService.getInstance().predictWord(keyword, Integer.parseInt(limit));
        //res.body(urls.toString());
    }

    @Route(path = "/snapshot", method = "POST")
    private void getSnapshot(Request req, Response res) throws IOException {
        log.info("[search] Getting snapshot");
        String requestBody = req.body();
        String normalizedUrl;
        try {
            JSONObject json = new JSONObject(requestBody); // Assuming JSON body
            normalizedUrl = Hasher.hash(json.getString("url"));
        } catch (JSONException e) {
            log.error("[search] Invalid request payload", e);
            res.status(400,"Bad Request"); // Bad Request
            res.body("Error: Invalid request payload");
            return;
        }
        System.out.println("normalizedUrl"+normalizedUrl);
        String page = SEARCH_SERVICE.getSnapshot(normalizedUrl);
        res.type("text/html");
        res.body(page);
    }


    @Route(path = "/phrase", method = "GET")
    private void getPhrase(Request req, Response res) throws IOException {
        log.info("[search] Getting best");
        String keyword = req.queryParams("keyword");
        String limit = req.queryParams("limit") == null ? "10" : req.queryParams("limit");

        List<String> lammatized = Parser.getLammelizedWords(keyword);

        if(lammatized.size() > 5){
            lammatized = lammatized.subList(0, 5);
        }

        Map<String, List<Integer>> sortedUrlWithPositions = SEARCH_SERVICE.calculateSortedPosition(lammatized);

        String json = OBJECT_MAPPER.writeValueAsString(sortedUrlWithPositions);
        res.body(json);
        res.type("application/json");
    }


    @Route(path = "/search", method = "GET")
    private void searchByKeywords(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        log.info("[search] Searching by query: " + query);

        // check weather
        if (query.toLowerCase().contains("weather")) {
            try {
                Map<String, Object> weatherData = WeatherService.getInstance().getWeatherInfo();
                String json = OBJECT_MAPPER.writeValueAsString(weatherData);
                res.body(json);
                res.type("application/json");
                return;
            } catch (IOException e) {
                log.error("[search] Error fetching weather data", e);
            }
        }




//        Map<String, Double> queryTfidf = SearchService.getInstance().calculateQueryTFIDF(query);
        Map<String, Double> queryTfidf = SEARCH_SERVICE.calculateQueryTF(query);
        queryTfidf.forEach((word,score)->{
            log.info("[search] queryTfidf in query: " + word + " score: " + score);
        });

        //! use searchByKeyword for now, should be searchByKeywords
        Map<String, Set<Integer>> urlsWithPositions = SEARCH_SERVICE.searchByKeyword(query);
        urlsWithPositions.forEach((url,positions)->{
            log.info("[search] URL: " + url + " | Positions: " + positions);
        });

        // use best position
        List<String> queryTokens = Parser.getLammelizedWords(query);
        Map<String, List<Integer>> bestPositions = SEARCH_SERVICE.calculateSortedPosition(queryTokens);
        double phraseMatchScore = SEARCH_SERVICE.calculatePhraseMatchScore(queryTokens, bestPositions);

        List<Map<String, Object>> results = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> entry : urlsWithPositions.entrySet()) {
            String hashedUrl = entry.getKey();

            // get icon, context, title, host, url for the FE
            Map<String, String> pageDetails = SEARCH_SERVICE.getPageDetails(hashedUrl);
            String pageContent = pageDetails.get("pageContent");
            String icon = pageDetails.get("icon");
            String url = pageDetails.get("url");
            String host = SEARCH_SERVICE.extractHostName(url);
            String title =pageDetails.get("title");

            // get best positions
            String contextSnippet = SEARCH_SERVICE.generateSnippetFromPositions(pageContent,
                    bestPositions.get(hashedUrl), 60);

            // Calculate title+og description weight
            double titleOGMatchScore = SEARCH_SERVICE.calculateTitleAndOGMatchScore(hashedUrl, query);

            // Calculate TF-IDF vector
//            Map<String, Double> docTfidf = SearchService.getInstance().calculateDocumentTFIDF(url, entry.getValue());
            Map<String, Double> docTfidf = SEARCH_SERVICE.calculateDocumentTF(hashedUrl, query);

            // Calculate cosine similarity between query and document TF-IDF vectors
//            double tfidfSimilarity = SearchService.getInstance().cosineSimilarity(queryTfidf, docTfidf);
            double tfidfSimilarity = SEARCH_SERVICE.calculateTFIDF(queryTfidf, docTfidf);

            // Get the PageRank score
            double pageRank = SEARCH_SERVICE.getPagerank(hashedUrl);

            // Combine scores with weighting (alpha for TF-IDF similarity, (1 - alpha) for PageRank)
            double combinedScore = tfIDFWeight * tfidfSimilarity +
                    pgrkWeight * pageRank +
                    titleDespMatchWeight * (titleOGMatchScore) +
                    phraseMatchWeight * (phraseMatchScore);


            Map<String, Object> result = new HashMap<>();
            result.put("title", title);
            result.put("url", url);
            result.put("combinedScore", combinedScore);
            result.put("host", host);
            result.put("icon", icon);
            result.put("context", contextSnippet);

            results.add(result);

            log.info("[search] URL: " + hashedUrl + ", TF-IDF: " + tfidfSimilarity +
                    ", PageRank: " + pageRank + ", phraseMatchScore: " + phraseMatchScore +
                    ", titleOGMatchScore: " + titleOGMatchScore + ", Combined Score: " + combinedScore);
        }

        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));
        String json = OBJECT_MAPPER.writeValueAsString(results);
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/search/image", method = "GET")
    private void searchImage(Request req, Response res) throws IOException {
        String keyword = req.queryParams("query");
        log.info("[search] Searching images for keyword: " + keyword);

        if (keyword == null || keyword.isEmpty()) {
            res.status(400, "Keyword cannot be empty");
            return;
        }

        List<String> imageUrls = SEARCH_SERVICE.getImages(keyword);

        if (imageUrls.isEmpty()) {
            log.warn("[search] No images found for keyword: " + keyword);
            res.status(404, "No images found for the given keyword");
            return;
        }

        String json = OBJECT_MAPPER.writeValueAsString(imageUrls);
        res.body(json);
        res.type("application/json");
        log.info("[search] Returning " + imageUrls.size() + " images for keyword: " + keyword);
    }

    @Route(path = "/search/pagelink", method = "GET")
    private void searchPageLink(Request req, Response res) throws IOException {
        String linkId = req.queryParams("query");
        log.info("[search] Searching link by id: " + linkId);

        if (linkId == null || linkId.isEmpty()) {
            res.status(400, "Keyword cannot be empty");
            return;
        }

        String actualLink = SEARCH_SERVICE.getLinkFromID(linkId);
        System.out.println("actualLink"+actualLink);
        if (actualLink.isEmpty()) {
            log.warn("[search] No url found for keyword: " + linkId);
            res.status(404, "No images found for the given keyword");
            return;
        }

        String json = OBJECT_MAPPER.writeValueAsString(actualLink);
        res.body(json);
        res.type("application/json");
        log.info("[search] Returning " + actualLink + " images for keyword: " + linkId);
    }


    private static final ExecutorService executor = Executors.newFixedThreadPool(20);
    private static final int TIMEOUT_SECONDS = 5;

    static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

    @Route(path = "/search/v2", method = "GET")
    private void searchByKeywordsV2(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        List<String> queryTokens = Parser.getLammelizedWords(query);

        log.info("[search] Searching by query: " + query);

        // Check weather
        if (query.toLowerCase().contains("weather")) {
            try {
                Map<String, Object> weatherData = WeatherService.getInstance().getWeatherInfo();
                String json = OBJECT_MAPPER.writeValueAsString(weatherData);
                res.body(json);
                res.type("application/json");
                return;
            } catch (IOException e) {
                log.error("[search] Error fetching weather data", e);
            }
        }

        // Calculate query TF-IDF
        CompletableFuture<Map<String, Double>> queryTfidfFuture = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return SEARCH_SERVICE.calculateQueryTF(query);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

//        // Search by keyword
//        CompletableFuture<Map<String, Map<String, List<Integer>>>> urlsWithPositionsFuture = CompletableFuture.supplyAsync(
//                () -> {
//                    try {
//                        return SEARCH_SERVICE.searchByKeywordsIntersection(queryTokens);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }, executor);
//
//        // Calculate best positions
//        CompletableFuture<Map<String, List<Integer>>> bestPositionsFuture = queryTfidfFuture.thenApplyAsync(queryTfidf -> {
//            try {
//                return SEARCH_SERVICE.calculateSortedPosition(queryTokens);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }, executor);


        // Step 1: Search by keywords
        CompletableFuture<Map<String, Map<String, List<Integer>>>> urlsWithPositionsFuture =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return SEARCH_SERVICE.searchByKeywordsIntersection(queryTokens, 200);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

        // Step 2: Use the results of queryTfidfFuture and urlsWithPositionsFuture
        CompletableFuture<Map<String, List<Integer>>> bestPositionsFuture = urlsWithPositionsFuture.thenCompose(urlsWithPositions -> {
            return queryTfidfFuture.thenApplyAsync(queryTfidf -> {
                try {
                    return SEARCH_SERVICE.calculateSortedPosition(queryTokens, urlsWithPositions);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, executor);
        });

        // Wait for all futures to complete with a timeout
        Map<String, Double> queryTfidf = getWithTimeout(queryTfidfFuture, new HashMap<>());
        Map<String, Map<String,List<Integer>>> urlsWithPositions = getWithTimeout(urlsWithPositionsFuture, new HashMap<>());
        Map<String, List<Integer>> bestPositions = getWithTimeout(bestPositionsFuture, new HashMap<>());


        List<Map<String, Object>> results = new ArrayList<>();

        // Process each URL in parallel
        urlsWithPositions.entrySet().parallelStream().forEach(entry -> {
            String hashedUrl = entry.getKey();

            try {
                // Fetch page details

                //System.out.println("pageContent: " + pageContent + " hashedUrl: " + hashedUrl);

//                Map<String, String> pageDetails = getWithTimeout(pageDetailsFuture, new HashMap<>());
//                String pageContent = pageDetails.getOrDefault("pageContent", "");
//                String icon = pageDetails.getOrDefault("icon", "");
//                String url = pageDetails.getOrDefault("url", "");
//                String host = SEARCH_SERVICE.extractHostName(url);
//                String title = pageDetails.getOrDefault("title", "");

                // Calculate scores in parallel
                //String snippet = SEARCH_SERVICE.generateSnippetFromPositions(pageContent, bestPositions.get(hashedUrl), 60);



                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);

                CompletableFuture<Double> titleOGMatchScoreFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateTitleAndOGMatchScore(row, queryTokens);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                CompletableFuture<Map<String, Double>> docTfidfFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateDocumentTF(hashedUrl, queryTokens);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                CompletableFuture<Double> pageRankFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.getPagerank(hashedUrl);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                // Get results with timeout
                double titleOGMatchScore = getWithTimeout(titleOGMatchScoreFuture, 0.0);
                Map<String, Double> docTfidf = getWithTimeout(docTfidfFuture, new HashMap<>());
                double pageRank = getWithTimeout(pageRankFuture, 0.0);

                //System.out.println("contextSnippet: " + snippet + " titleOGMatchScore: " + titleOGMatchScore + " pageRank: " + pageRank);

                // Calculate TF-IDF similarity
                double tfidfSimilarity = SEARCH_SERVICE.calculateTFIDF(queryTfidf, docTfidf);

                // Combine scores
                double combinedScore = tfIDFWeight * tfidfSimilarity +
                        pgrkWeight * pageRank +
                        titleDespMatchWeight * titleOGMatchScore +
                        phraseMatchWeight * SEARCH_SERVICE.calculatePhraseMatchScore(Arrays.asList(query.split("\\s+")), entry.getValue());

                // Add result
                Map<String, Object> result = new HashMap<>();
//                result.put("title", title);
//                result.put("url", url);
                result.put("combinedScore", combinedScore);
//                result.put("host", host);
//                result.put("icon", icon);
                //result.put("context", snippet);
                result.put("hashed", hashedUrl);


                //synchronized (results) {
                    results.add(result);
                //}

                log.info("[search] URL: " + hashedUrl + ", Combined Score: " + combinedScore);
            } catch (Exception e) {
                log.error("[search] Error processing URL: " + hashedUrl, e);
            }
        });

        // Sort results by combined score
        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));


        List<Map<String, Object>> view = results.subList(offset, Math.min(offset + limit, results.size()));

        view.forEach(result -> {
            log.info("[search] URL: " + result.get("hashed") + ", Combined Score: " + result.get("combinedScore"));
            String hashedUrl = (String) result.get("hashed");
            try {
                String content = SEARCH_SERVICE.getPageContent(hashedUrl);
                String snippet = SEARCH_SERVICE.generateSnippetFromPositions(content, bestPositions.get(hashedUrl), context_view);
                result.put("context", snippet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        });

        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(view);
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/search/v3", method = "GET")
    private void searchByKeywordsV3(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        double tfIDFWeight = (req.queryParams("tf") == null) ? 0.4 : Double.parseDouble(req.queryParams("tf"));
        double pgrkWeight = (req.queryParams("pr") == null) ? 0.14 : Double.parseDouble(req.queryParams("pg"));
        double titleDespMatchWeight = (req.queryParams("tt") == null) ? 0.16 : Double.parseDouble(req.queryParams("tt"));

        List<String> queryTokens = Parser.getLammelizedWords(query);

        Map<String, Row> keywordRows = new HashMap<>();

        for(String keyword : queryTokens){
            Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), keyword);
            if(row == null){
                log.warn("[search] No results found for keyword: " + keyword);
                continue;
            }
            keywordRows.put(keyword, row);
        }


        log.info("[search] Searching by query: " + query);

        // Check weather
        if (query.toLowerCase().contains("weather")) {
            try {
                Map<String, Object> weatherData = WeatherService.getInstance().getWeatherInfo();
                String json = OBJECT_MAPPER.writeValueAsString(weatherData);
                res.body(json);
                res.type("application/json");
                return;
            } catch (IOException e) {
                log.error("[search] Error fetching weather data", e);
            }
        }

        // row cached
        CompletableFuture<Map<String, Map<String, List<Integer>>>> urlsWithPositionsFuture =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return SEARCH_SERVICE.searchByKeywordsIntersection(keywordRows, queryTokens, 200);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);



        // Wait for all futures to complete with a timeout
        Map<String, Map<String,List<Integer>>> urlsWithPositions = getWithTimeout(urlsWithPositionsFuture, new HashMap<>());

        CompletableFuture<Map<String, List<Integer>>> bestPositionsFuture = CompletableFuture.supplyAsync(() ->
        {
            try {
                return SEARCH_SERVICE.calculateSortedPosition(queryTokens, urlsWithPositions);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, executor);

        Map<String, List<Integer>> bestPositions = getWithTimeout(bestPositionsFuture, new HashMap<>());
        List<Map<String, Object>> results = new ArrayList<>();

        // Process each URL in parallel
        urlsWithPositions.entrySet().parallelStream().forEach(entry -> {
            String hashedUrl = entry.getKey();

            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);

                // Calculate query TF-IDF
                CompletableFuture<Map<String, Double>> queryTfidfFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return SEARCH_SERVICE.calculateQueryTF(keywordRows);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executor);

                CompletableFuture<Double> titleOGMatchScoreFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateTitleAndOGMatchScore(row, queryTokens);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                CompletableFuture<Map<String, Double>> docTfidfFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateDocumentTF(hashedUrl, keywordRows);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);



                CompletableFuture<Double> pageRankFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.getPagerank(hashedUrl);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                // Get results with timeout
                double titleOGMatchScore = titleDespMatchWeight == 0.0 ? 0 : getWithTimeout(titleOGMatchScoreFuture, 0.0);
                Map<String, Double> docTfidf = tfIDFWeight == 0.0 ? new HashMap<>() : getWithTimeout(docTfidfFuture, new HashMap<>());
                double pageRank = pgrkWeight == 0 ? 0.0 : getWithTimeout(pageRankFuture, 0.0);
                Map<String, Double> queryTfidf = tfIDFWeight == 0.0 ? new HashMap<>(): getWithTimeout(queryTfidfFuture, new HashMap<>());

                //System.out.println("contextSnippet: " + snippet + " titleOGMatchScore: " + titleOGMatchScore + " pageRank: " + pageRank);

                // Calculate TF-IDF similarity
                double tfidfSimilarity = SEARCH_SERVICE.calculateTFIDF(queryTfidf, docTfidf);

                // Combine scores
                double combinedScore = tfIDFWeight * tfidfSimilarity +
                        pgrkWeight * pageRank +
                        titleDespMatchWeight * titleOGMatchScore +
                        phraseMatchWeight * SEARCH_SERVICE.calculatePhraseMatchScore(Arrays.asList(query.split("\\s+")), entry.getValue());

                // Add result
                Map<String, Object> result = new HashMap<>();
//                result.put("title", title);
//                result.put("url", url);
                result.put("combinedScore", combinedScore);
//                result.put("host", host);
//                result.put("icon", icon);
                //result.put("context", snippet);
                result.put("hashed", hashedUrl);


                //synchronized (results) {
                results.add(result);
                //}

                log.info("[search] URL: " + hashedUrl + ", Combined Score: " + combinedScore);
            } catch (Exception e) {
                log.error("[search] Error processing URL: " + hashedUrl, e);
            }
        });

        // Sort results by combined score
        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));


        List<Map<String, Object>> view = results.subList(offset, Math.min(offset + limit, results.size()));

        view.forEach(result -> {
            log.info("[search] URL: " + result.get("hashed") + ", Combined Score: " + result.get("combinedScore"));
            String hashedUrl = (String) result.get("hashed");
            try {
                String content = SEARCH_SERVICE.getPageContent(hashedUrl);
                String snippet = SEARCH_SERVICE.generateSnippetFromPositions(content, bestPositions.get(hashedUrl), context_view);
                result.put("context", snippet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        });

        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(view);
        res.body(json);
        res.type("application/json");
    }


    @Route(path = "/search/v4", method = "GET")
    private void searchByKeywordsV4(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        double tfIDFWeight = (req.queryParams("tf") == null) ? 0.4 : Double.parseDouble(req.queryParams("tf"));
        double pgrkWeight = (req.queryParams("pr") == null) ? 0.14 : Double.parseDouble(req.queryParams("pg"));
        double titleDespMatchWeight = (req.queryParams("tt") == null) ? 0.16 : Double.parseDouble(req.queryParams("tt"));

        List<String> queryTokens = Parser.getLammelizedWords(query);
        log.info("[search] Searching by query: " + query);

        Map<String, List<Integer>> bestPositions = SEARCH_SERVICE.calculateSortedPosition(queryTokens);
        double phraseMatchScore = SEARCH_SERVICE.calculatePhraseMatchScore(queryTokens, bestPositions);

        Map<String, Row> keywordRows = new HashMap<>();

        Set<String> mergedUrlIds = null;

        for(String keyword : queryTokens){
            System.out.println("keyword: " + keyword);
            Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.index"), keyword);
            if(row == null){
                log.warn("[search] No results found for keyword: " + keyword);
                continue;
            }
            keywordRows.put(keyword, row);
            if(mergedUrlIds == null){
                mergedUrlIds = row.columns() == null ? new HashSet<>() : row.columns();
            }else{
                mergedUrlIds.retainAll(row.columns());
            }
        }
        if(mergedUrlIds == null || mergedUrlIds.isEmpty()){
            log.warn("[search] No results found for keyword: " + query);
            return;
        }

        Map<String, String> idToHashedUrl = new HashMap<>();
        Map<String, String> hashedUrlToId = new HashMap<>();

        // load urlid idurl cache from SearchService
        for (String urlId : mergedUrlIds) {
            String hashedUrl = SearchService.ID_TO_URL_CACHE.get(urlId);
            if (hashedUrl != null) {
                idToHashedUrl.put(urlId, hashedUrl);
                hashedUrlToId.put(hashedUrl, urlId);
            } else {
                log.warn("[search] ID not found in cache: " + urlId);
            }
        }

        // NOTICE: ADD CACHE FOR THIS PART LATER!
//        mergedUrlIds.forEach(urlId -> {
//            //System.out.println("urlId: " + urlId);
//            try {
//                byte[] rowByte = KVS_CLIENT.get(PropertyLoader.getProperty("table.id-url"), urlId, "value");
//                String hashedUrl = new String(rowByte);
//                idToHashedUrl.put(urlId, hashedUrl);
//                hashedUrlToId.put(hashedUrl, urlId);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });

        long start = System.currentTimeMillis();
        System.out.println("Start fetching page rank");

        SortedMap<String, Double> sortedUrlsMap = SEARCH_SERVICE.getPageRanks(hashedUrlToId.keySet(),200);

        System.out.println("sortedUrlsMap: " + sortedUrlsMap.size());

        long end = System.currentTimeMillis();

        System.out.println("End fetching page rank: " + (end - start) + " ms");
        // Check weather
        if (query.toLowerCase().contains("weather")) {
            try {
                Map<String, Object> weatherData = WeatherService.getInstance().getWeatherInfo();
                String json = OBJECT_MAPPER.writeValueAsString(weatherData);
                res.body(json);
                res.type("application/json");
                return;
            } catch (IOException e) {
                log.error("[search] Error fetching weather data", e);
            }
        }

        // Wait for all futures to complete with a timeout

        List<Map<String, Object>> results = new ArrayList<>();

        // Process each URL in parallel
        sortedUrlsMap.entrySet().parallelStream().forEach(entry -> {
            String hashedUrl = entry.getKey();

            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);

                // Calculate query TF-IDF
                CompletableFuture<Map<String, Double>> queryTfidfFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return SEARCH_SERVICE.calculateQueryTF(keywordRows);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executor);

                CompletableFuture<Double> titleOGMatchScoreFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateTitleAndOGMatchScore(row, queryTokens);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

//                CompletableFuture<Double> phraseMatchScoreFuture = CompletableFuture.supplyAsync(() ->
//                {
//                    try {
//                        return SEARCH_SERVICE.calculatePhraseMatchScore(row, queryTokens);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }, executor);


                CompletableFuture<Map<String, Double>> docTfidfFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateDocumentTF(hashedUrl, keywordRows);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                // Get results with timeout
                double titleOGMatchScore = titleDespMatchWeight == 0.0 ? 0 : getWithTimeout(titleOGMatchScoreFuture, 0.0);
//                double phraseMatchScore = phraseMatchWeight == 0.0 ? 0 : getWithTimeout(phraseMatchScoreFuture, 0.0);
                Map<String, Double> docTfidf = tfIDFWeight == 0.0 ? new HashMap<>() : getWithTimeout(docTfidfFuture, new HashMap<>());
                Map<String, Double> queryTfidf = tfIDFWeight == 0.0 ? new HashMap<>(): getWithTimeout(queryTfidfFuture, new HashMap<>());
                double pageRank = entry.getValue();

                // Calculate TF-IDF similarity
                double tfidfSimilarity = SEARCH_SERVICE.calculateTFIDF(queryTfidf, docTfidf);

                // Combine scores
                double combinedScore = tfIDFWeight * tfidfSimilarity +
                        pgrkWeight * pageRank +
                        titleDespMatchWeight * titleOGMatchScore +
                        phraseMatchWeight * phraseMatchScore;

                // Add result
                Map<String, Object> result = new HashMap<>();
//                result.put("title", title);
//                result.put("url", url);
                result.put("combinedScore", combinedScore);
//                result.put("host", host);
//                result.put("icon", icon);
                //result.put("context", snippet);
                result.put("hashed", hashedUrl);
                results.add(result);

                log.info("[search] URL: " + hashedUrl + ", Combined Score: " + combinedScore);
            } catch (Exception e) {
                log.error("[search] Error processing URL: " + hashedUrl, e);
            }
        });

        // Sort results by combined score
        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));


        List<Map<String, Object>> view = results.subList(offset, Math.min(offset + limit, results.size()));

        view.forEach(result -> {
            log.info("[search] URL: " + result.get("hashed") + ", Combined Score: " + result.get("combinedScore"));
            String hashedUrl = (String) result.get("hashed");
            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);
                String content = row.get("text");
                String title = row.get("title");
                String url = row.get("url");
                String icon = row.get("icon");
                String host = SEARCH_SERVICE.extractHostName(url);


                result.put("title", title);
                result.put("url", url);
                result.put("icon", icon);
                result.put("host", host);

                Map<String, List<Integer>> positions = SEARCH_SERVICE.getKeywordPositions(idToHashedUrl, keywordRows, hashedUrlToId.get(hashedUrl));

                List<Integer> best = SEARCH_SERVICE.getBestPositionWithSorted(queryTokens, positions, queryTokens.size() + 5, 50);
                String snippet = SEARCH_SERVICE.generateSnippetFromPositions(content, best, context_view);

                result.put("context", snippet);
                //result.put("context", snippet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        });

        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(view);
        res.body(json);
        res.type("application/json");
    }


    private static final Map<String, SoftReference<List<Map<String, Object>>>> QUERY_PAGE_CACHE = new HashMap<>();

    @Route(path = "/search/v5", method = "GET")
    private void searchByKeywordsV5(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));

        int pageLimit = (req.queryParams("pageLimit") == null) ? 200 : Integer.parseInt(req.queryParams("pageLimit"));
        int spanLimit = (req.queryParams("span") == null) ? 50 : Integer.parseInt(req.queryParams("context"));
        int snippetLimit = (req.queryParams("snippet") == null) ? 60 : Integer.parseInt(req.queryParams("snippet"));

        double tfIDFWeight = (req.queryParams("tf") == null) ? 0.4 : Double.parseDouble(req.queryParams("tf"));
        double pgrkWeight = (req.queryParams("pr") == null) ? 0.14 : Double.parseDouble(req.queryParams("pg"));
        double titleDespMatchWeight = (req.queryParams("tt") == null) ? 0.16 : Double.parseDouble(req.queryParams("tt"));

        List<String> queryTokens = Parser.getLammelizedWords(query);
        String token = String.join(" ", queryTokens);

        while(QUERY_PAGE_CACHE.containsKey(token)){
            System.out.println("Query token hits");
            var cachedValue = QUERY_PAGE_CACHE.get(token).get();
            if(cachedValue == null){
                QUERY_PAGE_CACHE.remove(token);
                break;
            }
            List<Map<String, Object>> view = cachedValue.subList(offset, Math.min(offset + limit, cachedValue.size()));
            String json = OBJECT_MAPPER.writeValueAsString(view);
            res.body(json);
            res.type("application/json");
            return;
        }


        log.info("[search] Searching by query: " + query);

//        Map<String, List<Integer>> bestPositions = SEARCH_SERVICE.calculateSortedPosition(queryTokens);
//        double phraseMatchScore = SEARCH_SERVICE.calculatePhraseMatchScore(queryTokens, bestPositions);

        Map<String, Row> keywordRows = new HashMap<>();

        Set<String> mergedUrlIds = null;

        for(String keyword : queryTokens){
            System.out.println("keyword: " + keyword);
            Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.index"), keyword);
            if(row == null){
                log.warn("[search] No results found for keyword: " + keyword);
                continue;
            }
            keywordRows.put(keyword, row);
            if(mergedUrlIds == null){
                mergedUrlIds = row.columns() == null ? new HashSet<>() : row.columns();
            }else{
                mergedUrlIds.retainAll(row.columns());
            }
        }
        if(mergedUrlIds == null || mergedUrlIds.isEmpty()){
            log.warn("[search] No results found for keyword: " + query);
            return;
        }

        Map<String, String> idToHashedUrl = new HashMap<>();
        Map<String, String> hashedUrlToId = new HashMap<>();

        // load urlid idurl cache from SearchService
        for (String urlId : mergedUrlIds) {
            String hashedUrl = SearchService.ID_TO_URL_CACHE.get(urlId);
            if (hashedUrl != null) {
                idToHashedUrl.put(urlId, hashedUrl);
                hashedUrlToId.put(hashedUrl, urlId);
            } else {
                log.warn("[search] ID not found in cache: " + urlId);
            }
        }

        // NOTICE: ADD CACHE FOR THIS PART LATER!
//        mergedUrlIds.forEach(urlId -> {
//            //System.out.println("urlId: " + urlId);
//            try {
//                byte[] rowByte = KVS_CLIENT.get(PropertyLoader.getProperty("table.id-url"), urlId, "value");
//                String hashedUrl = new String(rowByte);
//                idToHashedUrl.put(urlId, hashedUrl);
//                hashedUrlToId.put(hashedUrl, urlId);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });

        long start = System.currentTimeMillis();
        System.out.println("Start fetching page rank");

        SortedMap<String, Double> sortedUrlsMap = SEARCH_SERVICE.getPageRanksParallel(hashedUrlToId.keySet(),pageLimit, 500);

        System.out.println("sortedUrlsMap: " + sortedUrlsMap.size());

        long end = System.currentTimeMillis();

        System.out.println("End fetching page rank: " + (end - start) + " ms");
        // Check weather
        if (query.toLowerCase().contains("weather")) {
            try {
                Map<String, Object> weatherData = WeatherService.getInstance().getWeatherInfo();
                String json = OBJECT_MAPPER.writeValueAsString(weatherData);
                res.body(json);
                res.type("application/json");
                return;
            } catch (IOException e) {
                log.error("[search] Error fetching weather data", e);
            }
        }

        // Wait for all futures to complete with a timeout

        List<Map<String, Object>> results = new ArrayList<>();

        // Process each URL in parallel
        sortedUrlsMap.entrySet().parallelStream().forEach(entry -> {
            String hashedUrl = entry.getKey();

            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);

                // Calculate query TF-IDF
                CompletableFuture<Map<String, Double>> queryTfidfFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return SEARCH_SERVICE.calculateQueryTF(keywordRows);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executor);

                CompletableFuture<Double> titleOGMatchScoreFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateTitleAndOGMatchScore(row, queryTokens);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

//                CompletableFuture<Double> phraseMatchScoreFuture = CompletableFuture.supplyAsync(() ->
//                {
//                    try {
//                        return SEARCH_SERVICE.calculatePhraseMatchScore(row, queryTokens);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }, executor);


                CompletableFuture<Map<String, Double>> docTfidfFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateDocumentTF(hashedUrl, keywordRows);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                // Get results with timeout
                double titleOGMatchScore = titleDespMatchWeight == 0.0 ? 0 : getWithTimeout(titleOGMatchScoreFuture, 0.0);
//                double phraseMatchScore = phraseMatchWeight == 0.0 ? 0 : getWithTimeout(phraseMatchScoreFuture, 0.0);
                Map<String, Double> docTfidf = tfIDFWeight == 0.0 ? new HashMap<>() : getWithTimeout(docTfidfFuture, new HashMap<>());
                Map<String, Double> queryTfidf = tfIDFWeight == 0.0 ? new HashMap<>(): getWithTimeout(queryTfidfFuture, new HashMap<>());
                double pageRank = entry.getValue();

                // Calculate TF-IDF similarity
                double tfidfSimilarity = SEARCH_SERVICE.calculateTFIDF(queryTfidf, docTfidf);

                // Combine scores
                double combinedScore = tfIDFWeight * tfidfSimilarity +
                        pgrkWeight * pageRank +
                        titleDespMatchWeight * titleOGMatchScore ;
                        //phraseMatchWeight * phraseMatchScore;

                // Add result
                Map<String, Object> result = new HashMap<>();
//                result.put("title", title);
//                result.put("url", url);
                result.put("combinedScore", combinedScore);
//                result.put("host", host);
//                result.put("icon", icon);
                //result.put("context", snippet);
                result.put("hashed", hashedUrl);
                results.add(result);

                log.info("[search] URL: " + hashedUrl + ", Combined Score: " + combinedScore);
            } catch (Exception e) {
                log.error("[search] Error processing URL: " + hashedUrl, e);
            }
        });

        // Sort results by combined score
        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));



        QUERY_PAGE_CACHE.put(token, new SoftReference<>(results));

        List<Map<String, Object>> view = results.subList(offset, Math.min(offset + limit, results.size()));

        view.forEach(result -> {
            log.info("[search] URL: " + result.get("hashed") + ", Combined Score: " + result.get("combinedScore"));
            String hashedUrl = (String) result.get("hashed");
            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);
                String content = row.get("text");
                String title = row.get("title");
                String url = row.get("url");
                String icon = row.get("icon");
                String host = SEARCH_SERVICE.extractHostName(url);


                result.put("title", title);
                result.put("url", url);
                result.put("icon", icon);
                result.put("host", host);

                Map<String, List<Integer>> positions = SEARCH_SERVICE.getKeywordPositions(idToHashedUrl, keywordRows, hashedUrlToId.get(hashedUrl));

                List<Integer> best = SEARCH_SERVICE.getBestPositionWithSorted(queryTokens, positions, queryTokens.size() + 5, spanLimit);
                String snippet = SEARCH_SERVICE.generateSnippetFromPositions(content, best, snippetLimit);

                result.put("context", snippet);
                //result.put("context", snippet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        });

        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(view);
        res.body(json);
        res.type("application/json");
    }



    @Route(path = "/search/v6", method = "GET")
    private void searchByKeywordsV6(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));

        int pageLimit = (req.queryParams("pageLimit") == null) ? 200 : Integer.parseInt(req.queryParams("pageLimit"));
        int spanLimit = (req.queryParams("span") == null) ? 50 : Integer.parseInt(req.queryParams("context"));
        int snippetLimit = (req.queryParams("snippet") == null) ? 60 : Integer.parseInt(req.queryParams("snippet"));
        int forceDrop = (req.queryParams("forceDrop") == null) ? 500 : Integer.parseInt(req.queryParams("forceDrop"));

        double tfIDFWeight = (req.queryParams("tf") == null) ? 1 : Double.parseDouble(req.queryParams("tf"));
        double pgrkWeight = (req.queryParams("pr") == null) ? 0.5 : Double.parseDouble(req.queryParams("pg"));
        double titleDespMatchWeight = (req.queryParams("tt") == null) ? 2 : Double.parseDouble(req.queryParams("tt"));
        double phraseMatchWeight = (req.queryParams("pm") == null) ? 20 : Double.parseDouble(req.queryParams("pm"));


        List<String> queryTokens = Parser.getLammelizedWords(query);
        String token = String.join(" ", queryTokens);

        while(QUERY_PAGE_CACHE.containsKey(token)){
            System.out.println("Query token hits");
            var cachedValue = QUERY_PAGE_CACHE.get(token).get();
            if(cachedValue == null){
                QUERY_PAGE_CACHE.remove(token);
                break;
            }
            if (offset >= cachedValue.size()) {
                offset = 0; // Reset to the first page if offset is out of range
            }
            List<Map<String, Object>> view = cachedValue.subList(offset, Math.min(offset + limit, cachedValue.size()));

            view.forEach(result -> {
                processResult(result,snippetLimit);
            });

            String json = OBJECT_MAPPER.writeValueAsString(view);
            res.body(json);
            res.type("application/json");
            log.info("[search] Offset: " + offset + ", Limit: " + limit + ", Total Results: " + view.size());

            return;
        }


        log.info("[search] Searching by query: " + query);


        Map<String, Row> keywordRows = new ConcurrentHashMap<>();
        final Set<String>[] mergedUrlIds = new Set[]{null};

        List<CompletableFuture<Void>> futures = queryTokens.stream()
                .map(keyword -> CompletableFuture.runAsync(() -> {
                    System.out.println("keyword: " + keyword);
                    try {
                        Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.index"), keyword);
                        if (row == null) {
                            log.warn("[search] No results found for keyword: " + keyword);
                            return;
                        }
                        keywordRows.put(keyword, row);
                        synchronized (this) { // 确保 mergedUrlIds 的线程安全
                            if (mergedUrlIds[0] == null) {
                                mergedUrlIds[0] = new HashSet<>(row.columns() == null ? new HashSet<>() : row.columns());
                            } else {
                                mergedUrlIds[0].retainAll(row.columns());
                            }
                        }
                    } catch (Exception e) {
                        log.error("[search] Error fetching row for keyword: " + keyword, e);
                    }
                }))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        if (mergedUrlIds[0] == null || mergedUrlIds[0].isEmpty()) {
            log.warn("[search] No results found for query: " + query);
            return;
        }

        Map<String, String> idToHashedUrl = new HashMap<>();
        Map<String, String> hashedUrlToId = new HashMap<>();

        // load urlid idurl cache from SearchService
        for (String urlId : mergedUrlIds[0]) {
            String hashedUrl = SearchService.ID_TO_URL_CACHE.get(urlId);
            if (hashedUrl != null) {
                idToHashedUrl.put(urlId, hashedUrl);
                hashedUrlToId.put(hashedUrl, urlId);
            } else {
                log.warn("[search] ID not found in cache: " + urlId);
            }
        }

        // NOTICE: ADD CACHE FOR THIS PART LATER!
//        mergedUrlIds.forEach(urlId -> {
//            //System.out.println("urlId: " + urlId);
//            try {
//                byte[] rowByte = KVS_CLIENT.get(PropertyLoader.getProperty("table.id-url"), urlId, "value");
//                String hashedUrl = new String(rowByte);
//                idToHashedUrl.put(urlId, hashedUrl);
//                hashedUrlToId.put(hashedUrl, urlId);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });

        long start = System.currentTimeMillis();
        System.out.println("Start fetching page rank");

        SortedMap<String, Double> sortedUrlsMap = SEARCH_SERVICE.getPageRanksParallel(hashedUrlToId.keySet(),pageLimit, forceDrop);

        System.out.println("sortedUrlsMap: " + sortedUrlsMap.size());

        long end = System.currentTimeMillis();

        System.out.println("End fetching page rank: " + (end - start) + " ms");

        // Wait for all futures to complete with a timeout

        List<Map<String, Object>> results = new ArrayList<>();

        var urlToWordToPositions = SEARCH_SERVICE.searchByKeywordsIntersectionV2(idToHashedUrl, keywordRows, queryTokens, 100);

        // Process each URL in parallel
        sortedUrlsMap.entrySet().parallelStream().forEach(entry -> {
            String hashedUrl = entry.getKey();

            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);

                // Calculate query TF-IDF
                CompletableFuture<Map<String, Double>> queryTfidfFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return SEARCH_SERVICE.calculateQueryTF(keywordRows);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executor);

                CompletableFuture<Double> titleOGMatchScoreFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateTitleAndOGMatchScore(row, queryTokens);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                CompletableFuture<Map<String, Double>> docTfidfFuture = CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return SEARCH_SERVICE.calculateDocumentTF(hashedUrl, keywordRows);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                // Get results with timeout
                double titleOGMatchScore = titleDespMatchWeight == 0.0 ? 0 : getWithTimeout(titleOGMatchScoreFuture, 0.0);
//                double phraseMatchScore = phraseMatchWeight == 0.0 ? 0 : getWithTimeout(phraseMatchScoreFuture, 0.0);
                Map<String, Double> docTfidf = tfIDFWeight == 0.0 ? new HashMap<>() : getWithTimeout(docTfidfFuture, new HashMap<>());
                Map<String, Double> queryTfidf = tfIDFWeight == 0.0 ? new HashMap<>(): getWithTimeout(queryTfidfFuture, new HashMap<>());
                double pageRank = entry.getValue();

                // Calculate TF-IDF similarity
                double tfidfSimilarity = SEARCH_SERVICE.calculateTFIDF(queryTfidf, docTfidf);


                    // ONE WORD LOGIC
//                for(String keyword : keywordRows.keySet()){
//                   var urlToPosition = SEARCH_SERVICE.searchByKeywordV2(keywordRows.get(keyword));
//                     keywordToUrlToPositions.put(keyword, urlToPosition);
//                }


                Map<List<Integer>, Double> phraseMatchScorePair = SEARCH_SERVICE.calculatePhraseMatchScoreV2(queryTokens, urlToWordToPositions.get(hashedUrl));

                double phraseMatchScore = phraseMatchScorePair.values().stream().mapToDouble(Double::doubleValue).sum();

                double combinedScore = calculateCombinedScore(tfidfSimilarity, 1.0,
                        pageRank, 1.0,
                        titleOGMatchScore, 1.0,
                        phraseMatchScore, 1.0,
                        tfIDFWeight,
                        pgrkWeight,
                        titleDespMatchWeight,
                        phraseMatchWeight);



                // Add result
                Map<String, Object> result = new HashMap<>();


                Optional<List<Integer>> optionalPosition = phraseMatchScorePair.keySet().stream().findFirst();
                if (optionalPosition.isPresent()) {
                    result.put("position", optionalPosition.get());
                } else {
                    result.put("position", new ArrayList<>());
                }

                result.put("combinedScore", combinedScore);

                result.put("hashed", hashedUrl);
                results.add(result);

                log.info("[search] URL: " + hashedUrl + ", Combined Score: " + combinedScore);
            } catch (Exception e) {
                log.error("[search] Error processing URL: " + hashedUrl, e);
            }
        });

        // Sort results by combined core
        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));

        QUERY_PAGE_CACHE.put(token, new SoftReference<>(results));

        List<Map<String, Object>> view = results.subList(offset, Math.min(offset + limit, results.size()));

        view.forEach(result -> {
            processResult(result,snippetLimit);
        });

        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(view);
        res.body(json);
        res.type("application/json");
    }

    private void processResult(Map<String, Object> result, int snippetLimit) {
        log.info("[search] URL: " + result.get("hashed") + ", Combined Score: " + result.get("combinedScore"));
        String hashedUrl = (String) result.get("hashed");

        try {
            // Fetch the row from the KVS client
            Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);

            // Extract and process data
            String content = row.get("text");
            String title = row.get("title");
            String url = row.get("url");
            String icon = row.get("icon");
            String host = SEARCH_SERVICE.extractHostName(url);

            // Update the result map with processed data
            result.put("title", title);
            result.put("url", url);
            result.put("icon", icon);
            result.put("host", host);

            // Get the best position for generating snippets
            List<Integer> bestPosition = result.get("position") == null
                    ? new ArrayList<>()
                    : (List<Integer>) result.get("position");

            // Generate a snippet and update the result
            String snippet = SEARCH_SERVICE.generateSnippetFromPositions(content, bestPosition, snippetLimit);
            result.put("context", snippet);

        } catch (IOException e) {
            log.error("[search] Error processing result for URL: " + hashedUrl, e);
            throw new RuntimeException(e); // Ensure failure is propagated
        }
    }

    private double normalize(double value, double min, double max) {
        if (min == max) {
            return 0.5;
        }
        return (value - min) / (max - min);
    }

    public double calculateCombinedScore(
            double tfidfSimilarity, double maxTfidfSimilarity,
            double pageRank, double maxPageRank,
            double titleOGMatchScore, double maxTitleOGMatchScore,
            double phraseMatchScore, double maxPhraseMatchScore,
            double tfIDFWeight, double pgrkWeight, double titleDespMatchWeight, double phraseMatchWeight) {

        // Normalize each score
        double normalizedTfidf = normalize(tfidfSimilarity, 0, maxTfidfSimilarity);
        double normalizedPageRank = normalize(pageRank, 0, maxPageRank);
        double normalizedTitleOGMatchScore = normalize(titleOGMatchScore, 0, maxTitleOGMatchScore);
        double normalizedPhraseMatchScore = normalize(phraseMatchScore, 0, maxPhraseMatchScore);

        double smoothTfidf = sigmoid(normalizedTfidf);
        double smoothPageRank = sigmoid(normalizedPageRank);
        double smoothTitleOGMatchScore = sigmoid(normalizedTitleOGMatchScore);
        double smoothPhraseMatchScore = sigmoid(normalizedPhraseMatchScore);


        // Normalize weights to ensure the sum is 1
        double totalWeight = tfIDFWeight + pgrkWeight + titleDespMatchWeight + phraseMatchWeight;
        tfIDFWeight /= totalWeight;
        pgrkWeight /= totalWeight;
        titleDespMatchWeight /= totalWeight;
        phraseMatchWeight /= totalWeight;

        // Combine normalized scores with weights
        return tfIDFWeight * smoothTfidf +
                pgrkWeight * smoothPageRank +
                titleDespMatchWeight * smoothTitleOGMatchScore +
                phraseMatchWeight * smoothPhraseMatchScore;
    }

    private double sigmoid(double value) {
        return 1 / (1 + Math.exp(-value));
    }

    // Helper method to handle timeout
    private <T> T getWithTimeout(CompletableFuture<T> future, T defaultValue) {
        try {
            return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Task timed out, returning default value");
            return defaultValue;
        }
    }
}

