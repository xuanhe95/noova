package org.noova.gateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.PDFService;
import org.noova.tools.Logger;
import org.noova.tools.Parser;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.*;

public class PDFController implements IController {

    private static final Logger log = Logger.getLogger(PDFController.class);

    private static PDFController instance;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final PDFService PDF_SERVICE = PDFService.getInstance();

    private PDFController() {
    }

    public static PDFController getInstance() {
        if (instance == null) {
            instance = new PDFController();
        }
        return instance;
    }

    /**
     * Endpoint to search PDFs by keywords.
     * Example usage: GET /pdf/search?query=keyword&limit=10&offset=0
     */
    @Route(path = "/pdf/search", method = "GET")
    private void searchPDFs(Request req, Response res) throws IOException {
        log.info("[PDF Search] Searching PDFs");
        String query = req.queryParams("query");

        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));

        if (query == null || query.isEmpty()) {
            log.warn("[PDF Search] Empty query received");
            res.body("{\"error\":\"Empty query\"}");
            res.type("application/json");
            return;
        }

        // Process query into keywords using lemmatization
        List<String> keywords = Parser.getLammelizedWords(query);

        log.info("[PDF Search] Keywords: " + keywords);

        // Search for PDFs matching the keywords
        Map<String, Map<String, String>> pdfResults = PDF_SERVICE.searchByKeywordsIntersection(keywords, offset, limit);

        // Convert results to JSON and respond
        respondWithJson(res, pdfResults);
    }

    /**
     * Endpoint to search PDFs asynchronously by keywords.
     * Example usage: GET /pdf/search-async?query=keyword&limit=10&offset=0
     */
    @Route(path = "/pdf/search-async", method = "GET")
    private void searchPDFsAsync(Request req, Response res) throws IOException {
        log.info("[PDF Search] Searching PDFs asynchronously");
        String query = req.queryParams("query");

        int limit = (req.queryParams("limit") == null) ? 10 : Integer.parseInt(req.queryParams("limit"));
        int offset = (req.queryParams("offset") == null) ? 0 : Integer.parseInt(req.queryParams("offset"));

        if (query == null || query.isEmpty()) {
            log.warn("[PDF Search] Empty query received");
            res.body("{\"error\":\"Empty query\"}");
            res.type("application/json");
            return;
        }

        // Process query into keywords using lemmatization
        List<String> keywords = Parser.getLammelizedWords(query);

        log.info("[PDF Search Async] Keywords: " + keywords);

        // Search for PDFs matching the keywords asynchronously
        Map<String, Map<String, String>> pdfResults = PDF_SERVICE.searchByKeywordsIntersectionAsync(keywords, offset, limit);

        // Convert results to JSON and respond
        respondWithJson(res, pdfResults);
    }

    /**
     * Helper method to respond with JSON data.
     */
    private void respondWithJson(Response res, Object data) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(data);
            res.body(json);
            res.type("application/json");
        } catch (Exception e) {
            log.error("[PDFController] Error generating JSON response: " + e.getMessage());
            res.status(500, "Server Error");
            res.body("{\"error\":\"Failed to generate JSON response\"}");
        }
    }
}
