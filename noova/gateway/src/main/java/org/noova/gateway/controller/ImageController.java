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

    @Route(path = "/image/key", method = "GET")
    private void searchByKeyword(Request req, Response res) throws IOException {
        log.info("[search] Searching by keyword");
        String keyword = req.queryParams("keyword");

        log.info("[search] Searching by keyword: " + keyword);

        var imageMap = IMAGE_SERVICE.searchByKeyword(keyword, 0, 10);


        String json = OBJECT_MAPPER.writeValueAsString(imageMap);
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/image/keys", method = "GET")
    private void searchByKeywords(Request req, Response res) throws IOException {
        log.info("[search] Searching by keywords");
        String keyword = req.queryParams("keyword");


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
                System.out.println("images: " + images);

            }
        }

        List<Map.Entry<String, Integer>> sortedImages = keywordCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .toList();


        SortedMap<String, Set<String>> result = new TreeMap<>();

        for (int i = 0; i < Math.min(sortedImages.size(), limit); i++) {
            result.put(sortedImages.get(i).getKey(), images.get(sortedImages.get(i).getKey()));
        }

        String json = OBJECT_MAPPER.writeValueAsString(result);
        res.body(json);
        res.type("application/json");
    }

}
