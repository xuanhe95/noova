package org.noova.gateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.ImageService;
import org.noova.gateway.service.SearchService;
import org.noova.tools.LemmaLoader;
import org.noova.tools.Logger;
import org.noova.tools.Parser;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Collectors;

public class ImageController implements IController{

    private static final Logger log = Logger.getLogger(SearchController.class);

    private static ImageController instance;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final ImageService IMAGE_SERVICE = ImageService.getInstance();



    private ImageController() {
    }

    public static ImageController getInstance() {
        if (instance == null) {
            instance = new ImageController();
        }
        return instance;
    }

//    @Route(path = "/image/key", method = "GET")
//    private void searchByKeyword(Request req, Response res) throws IOException {
//        log.info("[search] Searching by keyword");
//        String keyword = req.queryParams("keyword");
//
//        log.info("[search] Searching by keyword: " + keyword);
//
//        var imageMap = IMAGE_SERVICE.searchByKeyword(keyword, 0, 10);
//
//
//        String json = OBJECT_MAPPER.writeValueAsString(imageMap);
//        res.body(json);
//        res.type("application/json");
//    }


    @Route(path = "/image/key", method = "GET")
    private void searchByKeywordsIntersection(Request req, Response res) throws IOException {
        log.info("[search] Searching by keywords");
        String keyword = req.queryParams("query");

        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));

        if(keyword == null || keyword.isEmpty()){
            log.warn("[search] Empty keyword received");
            res.body("[error]");
            res.type("application/json");
            return;
        }

        List<String> keywords = Parser.getLammelizedWords(keyword);

        Map<String, Integer> keywordCount = new HashMap<>();

        Map<String, Set<String>> images= new HashMap<>();

        var imageMap = IMAGE_SERVICE.searchByKeywordsIntersection(keywords, 0, 10);

        for (String imageUrl : imageMap.keySet()) {
            keywordCount.put(imageUrl, keywordCount.getOrDefault(imageUrl, 0) + 1);
        }

        for (String imageUrl : imageMap.keySet()) {
            if (!images.containsKey(imageUrl)) {
                images.put(imageUrl, new HashSet<>());
            }
            images.get(imageUrl).addAll(imageMap.get(imageUrl));
        }

        List<Map.Entry<String, Integer>> sortedImages = keywordCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .toList();

        List<Map.Entry<String, Integer>> view = sortedImages.subList(offset, Math.min(offset + limit, sortedImages.size()));

        SortedMap<String, Set<String>> result = new TreeMap<>();

        view.forEach(rest->{
            result.put(rest.getKey(),images.get(rest.getKey()));
        });

        String json = OBJECT_MAPPER.writeValueAsString(result);
        res.body(json);
        res.type("application/json");
    }

    Map<String, SoftReference<List<Map.Entry<String, Integer>>>> IMAGE_CACHE = new HashMap<>();

    @Route(path = "/image/v2", method = "GET")
    private void searchByKeywordsIntersectionV2(Request req, Response res) throws IOException {
        log.info("[search] Searching by keywords");
        String keyword = req.queryParams("query");

        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));

        if(keyword == null || keyword.isEmpty()){
            log.warn("[search] Empty keyword received");
            res.body("[error]");
            res.type("application/json");
            return;
        }

        List<String> keywords = Parser.getLammelizedWords(keyword);

        String queryKey = String.join(" ", keywords);
        while (IMAGE_CACHE.containsKey(queryKey)) {
            SortedMap<String, Set<String>> result = new TreeMap<>();
            var sortedImages = IMAGE_CACHE.get(queryKey).get();
            if(sortedImages == null){
                break;
            }
            var view = sortedImages.subList(offset, Math.min(offset + limit, sortedImages.size()));
            respondWithJson(res, view);
            return;
        }


        Map<String, Integer> keywordCount = new HashMap<>();

        Map<String, Set<String>> images= new HashMap<>();

        var imageMap = IMAGE_SERVICE.searchByKeywordsIntersectionAsync(keywords, 0, 1000);

        imageMap.forEach((imageUrl, matchedKeywords) -> {
            keywordCount.merge(imageUrl, matchedKeywords.size(), Integer::sum);
            images.computeIfAbsent(imageUrl, k -> new HashSet<>()).addAll(matchedKeywords);
        });

        List<Map.Entry<String, Integer>> sortedImages = keywordCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .toList();

        List<Map.Entry<String, Integer>> view = sortedImages.subList(
                Math.min(offset, sortedImages.size()),
                Math.min(offset + limit, sortedImages.size())
        );

        SortedMap<String, Set<String>> result = new TreeMap<>();
        view.forEach(entry -> result.put(entry.getKey(), images.get(entry.getKey())));



        String json = OBJECT_MAPPER.writeValueAsString(result);
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/image/fuzzy", method = "GET")
    private void searchImageByKeywords(Request req, Response res) throws IOException {
        log.info("[search] Searching by images");
        String keyword = req.queryParams("query");

        int limit = (req.queryParams("limit") == null) ? 50 : Integer.parseInt(req.queryParams("limit"));
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        System.out.println( "limit: " + limit + " offset " + offset);

        if(keyword == null || keyword.isEmpty()){
            log.warn("[search] Empty image keyword received");
            res.body("[error]");
            res.type("application/json");
            return;
        }

        List<String> keywords = Parser.getLammelizedWords(keyword);

        Map<String, Integer> keywordCount = new HashMap<>();

        Map<String, Set<String>> images= new HashMap<>();



        for (int i = 0; i < Math.min(keywords.size(), 8); i++) {

            String key = keywords.get(i);
            if(key == null || key.isEmpty()){
                continue;
            }

            System.out.println("key: " + key);
            String lemma = LemmaLoader.getLemma(key);
            if (lemma != null) {
                var imageMap = IMAGE_SERVICE.searchByKeyword(lemma, 0, 10);
                for (String imageUrl : imageMap.keySet()) {
                    keywordCount.put(imageUrl, keywordCount.getOrDefault(imageUrl, 0) + 1);
                }

                for (String imageUrl : imageMap.keySet()) {
                    if (!images.containsKey(imageUrl)) {
                        images.put(imageUrl, new HashSet<>());
                    }
                    images.get(imageUrl).addAll(imageMap.get(imageUrl));
                }
                //System.out.println("images: " + images);

            }
        }

        List<Map.Entry<String, Integer>> sortedImages = keywordCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .toList();

        SortedMap<String, Set<String>> result = new TreeMap<>();
        if (offset>= sortedImages.size()) {
            respondWithJson(res, result);
            return;
        }
        int toIndex = Math.min(offset + limit, sortedImages.size());
        List<Map.Entry<String, Integer>> view = sortedImages.subList(offset, toIndex);

        view.forEach(entry -> result.put(entry.getKey(), images.get(entry.getKey())));

        respondWithJson(res, result);

    }

    private void respondWithJson(Response res, Object data) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(data);
            res.body(json);
            res.type("application/json");
        } catch (Exception e) {
            res.status(500,"Server Error");
            res.body("{\"error\":\"Failed to generate JSON response\"}");
        }
    }

}
