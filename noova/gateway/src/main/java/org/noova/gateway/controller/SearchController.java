package org.noova.gateway.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.SearchService;
import org.noova.gateway.service.Service;
import org.noova.kvs.Row;
import org.noova.tools.Logger;
import org.noova.tools.Parser;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.Request;
import org.noova.webserver.Response;
import org.noova.kvs.KVS;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Xuanhe Zhang
 */
public class SearchController implements IController {

    private static final Logger log = Logger.getLogger(SearchController.class);

    private static SearchController instance;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final SearchService SEARCH_SERVICE = SearchService.getInstance();

    private static final double alpha = 0.5; // Need more analysis
    private static final double titleDespMatchWeight = 0.3;

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

    @Route(path = "/snapshot", method = "GET")
    private void getSnapshot(Request req, Response res) throws IOException {
        log.info("[search] Getting snapshot");
        String normalizedUrl = req.body();

        String page = SEARCH_SERVICE.getSnapshot(normalizedUrl);
        res.type("text/html");
        res.body(page);
    }


    @Route(path = "/best", method = "GET")
    private void getBest(Request req, Response res) throws IOException {
        log.info("[search] Getting best");
        String keyword = req.queryParams("keyword");
        String limit = req.queryParams("limit") == null ? "10" : req.queryParams("limit");

        List<String> lammatized = Parser.getLammelizedWords(keyword);


        System.out.println("Lammatized: " + lammatized);

        Map<String, List<Integer>> urlWithPositions = SEARCH_SERVICE.calculatePosition(lammatized);

        urlWithPositions.forEach((url, position) -> {
            System.out.println("URL: " + url + " | Positions: " + position);
        });


        SortedMap<String, List<Integer>> sortedMap = new TreeMap<>((a, b) -> {
            int aSize = urlWithPositions.get(a).size();
            int bSize = urlWithPositions.get(b).size();
            int sizeComparison = Integer.compare(bSize, aSize);
            if (sizeComparison != 0) {
                return sizeComparison; // 优先按值排序
            }

            System.out.println("a: " + urlWithPositions.get(a).get(0) + " b: " + urlWithPositions.get(b).get(0));
            System.out.println("a2: " + urlWithPositions.get(a).get(urlWithPositions.get(a).size() - 1) + " b2: " + urlWithPositions.get(b).get(urlWithPositions.get(b).size() - 1));

            int aDiff = Math.abs(urlWithPositions.get(a).get(urlWithPositions.get(a).size() - 1) - urlWithPositions.get(a).get(0));
            int bDiff = Math.abs(urlWithPositions.get(b).get(urlWithPositions.get(b).size() - 1) - urlWithPositions.get(b).get(0));

            System.out.println("aDiff: " + aDiff + " bDiff: " + bDiff);

            if(aDiff == bDiff){
                return a.compareTo(b);
            }
            return aDiff - bDiff; // 次要按差值排序
        });

        System.out.println("URL Map: " + sortedMap + "size: " + urlWithPositions.size());

        sortedMap.putAll(urlWithPositions);



        System.out.println("Sorted Map: " + sortedMap + "size: " + sortedMap.size());



        String json = OBJECT_MAPPER.writeValueAsString(sortedMap);
        res.body(json);
        res.type("application/json");
    }


    @Route(path = "/search", method = "GET")
    private void searchByKeywords(Request req, Response res) throws IOException {
        log.info("[search] Searching by query");
        String query = req.queryParams("query");
        log.info("[search] Searching by query: " + query);

//        Map<String, Double> queryTfidf = SearchService.getInstance().calculateQueryTFIDF(query);
        Map<String, Double> queryTfidf = SearchService.getInstance().calculateQueryTF(query);
        queryTfidf.forEach((word,score)->{
            log.info("[search] queryTfidf in query: " + word + " score: " + score);
        });

        //! use searchByKeyword for now, should be searchByKeywords
        Map<String, Set<Integer>> urlsWithPositions = SearchService.getInstance().searchByKeyword(query);
        urlsWithPositions.forEach((url,positions)->{
            log.info("[search] URL: " + url + " | Positions: " + positions);
        });

        List<Map<String, Object>> results = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> entry : urlsWithPositions.entrySet()) {
            String hashedUrl = entry.getKey();
            Set<Integer> positions = entry.getValue();

            // Calculate title weight
            String title = SearchService.getInstance().getTitle(hashedUrl);
            double titleMatchScore = SearchService.getInstance().calculateTitleMatchScore(query, title);

            // Calculate og description weight
            double despMatchScore = SearchService.getInstance().calculateTitleMatchScore(hashedUrl, title);

            // Calculate TF-IDF vector
//            Map<String, Double> docTfidf = SearchService.getInstance().calculateDocumentTFIDF(url, entry.getValue());
            Map<String, Double> docTfidf = SearchService.getInstance().calculateDocumentTF(hashedUrl, query);

            // Calculate cosine similarity between query and document TF-IDF vectors
//            double tfidfSimilarity = SearchService.getInstance().cosineSimilarity(queryTfidf, docTfidf);
            double tfidfSimilarity = SearchService.getInstance().calculateTFIDF(queryTfidf, docTfidf);

            // Get the PageRank score
            double pageRank = SearchService.getInstance().getPagerank(hashedUrl);

            // Combine scores with weighting (alpha for TF-IDF similarity, (1 - alpha) for PageRank)
//            double combinedScore = alpha * tfidfSimilarity + (1-alpha) * pageRank;
            double combinedScore = alpha * tfidfSimilarity +
                    (1 - alpha - titleDespMatchWeight) * pageRank +
                    titleDespMatchWeight * (titleMatchScore+despMatchScore);


            String pageContent = SearchService.getInstance().getPageContent(hashedUrl);
            log.info("[search] page content: " + pageContent);

            String contextSnippet = SearchService.getInstance().ExtractContextSnippet(pageContent, positions, 60); // TBD, hardcoded
            log.info("[search] page contextSnippet: " + contextSnippet);

            Map<String, Object> result = new HashMap<>();
            result.put("title", title);
            result.put("url", hashedUrl);
            result.put("combinedScore", combinedScore);
            result.put("context", contextSnippet);

            results.add(result);

            log.info("[search] URL: " + hashedUrl + ", TF-IDF Similarity: " + tfidfSimilarity + ", PageRank: " + pageRank + ", Combined Score: " + combinedScore);
        }

        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));
        String json = OBJECT_MAPPER.writeValueAsString(results);
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/search/image", method = "GET")
    private void searchImage(Request req, Response res) throws IOException {
        String keyword = req.queryParams("keyword");
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

}
