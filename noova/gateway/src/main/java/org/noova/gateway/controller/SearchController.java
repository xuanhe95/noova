package org.noova.gateway.controller;


import org.noova.gateway.service.SearchService;
import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Xuanhe Zhang
 */
public class SearchController implements IController {

    private static final Logger log = Logger.getLogger(SearchController.class);

    private static SearchController instance;

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
        Map<String, Set<Integer>> urls = SearchService.getInstance().searchByKeyword(keyword);

        StringBuilder result = new StringBuilder();

        urls.forEach((normalizedUrl, position) -> {
            result.append(normalizedUrl).append(": ").append(position).append("\n");
        });

        res.body(result.toString());
    }

    @Route(path = "/search/predict", method = "GET")
    private void searchByKeywordPredict(Request req, Response res) throws IOException {
        log.info("[search] Predicting by keyword");
        String keyword = req.queryParams("keyword");
        List<String> urls = SearchService.getInstance().predict(keyword);
        res.body(urls.toString());
    }
}
