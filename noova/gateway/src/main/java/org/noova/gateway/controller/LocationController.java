package org.noova.gateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.LocationService;
import org.noova.gateway.service.SearchService;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.IPLocation;
import org.noova.tools.Logger;
import org.noova.tools.Parser;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


public class LocationController implements IController {

    private static final Logger log = Logger.getLogger(LocationController.class);

    private static LocationController instance;
    private static final SearchService SEARCH_SERVICE = SearchService.getInstance();
    static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String IP_TABLE = PropertyLoader.getProperty("table.ip");

    private LocationController() {
    }

    public static LocationController getInstance() {
        if (instance == null) {
            instance = new LocationController();
        }
        return instance;
    }

    @Route(path = "/api/location", method = "GET")
    private void getAutocompleteSuggestions(Request req, Response res) throws IOException {
        log.info("[Location] Location service request received");
        String ipaddr = req.ip();
        String location = LocationService.getZipcodeFromIP(ipaddr);

        log.info("location:" + location+ " ip:" + ipaddr);

        Map<String, Object> result = new HashMap<>();
        if (location != null) {
            String[] locationParts = location.split(",");
            result.put("zipcode", locationParts[0]);
            result.put("township", locationParts[1]);
        } else {
            result.put("zipcode", "UNKNOWN");
            result.put("township", "UNKNOWN");
        }

        String json = OBJECT_MAPPER.writeValueAsString(result);
        log.info("json:" + json);
        res.body(json);
        res.type("application/json");
    }

    @Route(path = "/api/location-search", method = "GET")
    private void search(Request req, Response res) throws IOException {
        log.info("[Location Search] Request received");
        String userIP = req.ip();
        String query = req.queryParams("q");
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));
        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        int pageLimit = (req.queryParams("pageLimit") == null) ? 200 : Integer.parseInt(req.queryParams("pageLimit"));

        List<String> queryTokens = Parser.getLammelizedWords(query);
        Map<String, Row> keywordRows = new ConcurrentHashMap<>();
        final Set<String>[] mergedUrlIds = new Set[]{null};

        List<CompletableFuture<Void>> futures = queryTokens.stream()
                .map(keyword -> CompletableFuture.runAsync(() -> {
                    try {
                        Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.index"), keyword);
                        if (row != null) {
                            keywordRows.put(keyword, row);
                            synchronized (this) {
                                if (mergedUrlIds[0] == null) {
                                    mergedUrlIds[0] = new HashSet<>(row.columns() == null ? new HashSet<>() : row.columns());
                                } else {
                                    mergedUrlIds[0].retainAll(row.columns());
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("[location-search] Error fetching row for keyword: " + keyword, e);
                    }
                }))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        if (mergedUrlIds[0] == null || mergedUrlIds[0].isEmpty()) {
            res.body("[]");
            return;
        }

        Map<String, String> idToHashedUrl = new HashMap<>();
        Map<String, String> hashedUrlToId = new HashMap<>();
        for (String urlId : mergedUrlIds[0]) {
            String hashedUrl = SearchService.ID_TO_URL_CACHE.get(urlId);
            if (hashedUrl != null) {
                idToHashedUrl.put(urlId, hashedUrl);
                hashedUrlToId.put(hashedUrl, urlId);
            }
        }

        String userZip = IPLocation.getZipFromIP(userIP);
        Row zipRow = null;
        if (userZip != null) {
            zipRow = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.ip"), userZip);
        }

        List<Map<String, Object>> results = new ArrayList<>();

        var urlToWordToPositions = SEARCH_SERVICE.searchByKeywordsIntersectionV2(idToHashedUrl, keywordRows, queryTokens, pageLimit);

        for (String hashedUrl : hashedUrlToId.keySet()) {
            try {
                Row row = KVS_CLIENT.getRow(PropertyLoader.getProperty("table.processed"), hashedUrl);
                if (row == null) continue;

                // calculate zip distance
                double locationScore = 0.0;
                if (zipRow != null ) {
                    try {
                        int userZipInt = Integer.parseInt(userZip);
                        // 获取文档的ZIP
                        String docZip = hashedUrlToId.get(hashedUrl);
                        if (docZip != null) {
                            int docZipInt = Integer.parseInt(docZip);

                            // 计算ZIP差值的绝对值
                            int diff = Math.abs(userZipInt - docZipInt);

                            // 根据差值计算分数
                            if (diff == 0) {
                                locationScore = 1.0;  // 完全相同
                            } else if (diff < 100) {
                                locationScore = 0.8;  // 非常接近
                            } else if (diff < 500) {
                                locationScore = 0.6;  // 较接近
                            } else if (diff < 1000) {
                                locationScore = 0.4;  // 有点远
                            } else if (diff < 2000) {
                                locationScore = 0.2;  // 很远
                            } else {
                                locationScore = 0.1;  // 非常远
                            }
                        }
                    } catch (NumberFormatException e) {
                        log.error("Error parsing ZIP codes", e);
                        locationScore = 0.0;
                    }
                }

                Map<List<Integer>, Double> phraseMatchScorePair = SEARCH_SERVICE.calculatePhraseMatchScoreV2(
                        queryTokens,
                        urlToWordToPositions.get(hashedUrl)
                );

                // combine
                double combinedScore = locationScore * 2.0 +
                        phraseMatchScorePair.values().stream().mapToDouble(Double::doubleValue).sum();

                Map<String, Object> result = new HashMap<>();
                result.put("combinedScore", combinedScore);
                result.put("hashed", hashedUrl);
                Optional<List<Integer>> optionalPosition = phraseMatchScorePair.keySet().stream().findFirst();
                result.put("position", optionalPosition.orElse(new ArrayList<>()));

                results.add(result);
            } catch (Exception e) {
                log.error("[location-search] Error processing URL: " + hashedUrl, e);
            }
        }

        results.sort((a, b) -> Double.compare((Double) b.get("combinedScore"), (Double) a.get("combinedScore")));
        List<Map<String, Object>> view = results.subList(offset, Math.min(offset + limit, results.size()));

        view.forEach(result -> {
            try {
                Map<String, String> details = SEARCH_SERVICE.getPageDetails((String) result.get("hashed"));
                result.putAll(details);

                if (!((List<Integer>)result.get("position")).isEmpty()) {
                    String snippet = SEARCH_SERVICE.generateSnippetFromPositions(
                            details.get("pageContent"),
                            (List<Integer>)result.get("position"),
                            60
                    );
                    result.put("context", snippet);
                }
            } catch (IOException e) {
                log.error("[location-search] Error processing result details", e);
            }
        });

        String json = OBJECT_MAPPER.writeValueAsString(view);
        res.body(json);
        res.type("application/json");
    }
}