package org.noova.gateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.SearchService;
import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class AutocompleteController implements IController {

    private static final Logger log = Logger.getLogger(AutocompleteController.class);

    private static AutocompleteController instance;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private AutocompleteController() {
    }

    public static AutocompleteController getInstance() {
        if (instance == null) {
            instance = new AutocompleteController();
        }
        return instance;
    }

    @Route(path = "/autocomplete", method = "GET")
    private void getAutocompleteSuggestions(Request req, Response res) throws IOException {
        log.info("[autocomplete] Autocomplete request received");

        String prefix = req.queryParams("prefix");
        String limitParam = "10"; //TBD hardcoded 10
        int limit = (limitParam != null) ? Integer.parseInt(limitParam) : 10; //TBD hardcoded 10

        if (prefix == null || prefix.isEmpty()) {
            log.warn("[autocomplete] Empty prefix received");
            res.body("[]");
            res.type("application/json");
            return;
        }

        log.info("[autocomplete] Fetching suggestions for prefix: " + prefix);
        List<String> suggestions = SearchService.getInstance().getAutocompleteSuggestions(prefix, limit);
        log.info("[autocomplete] suggestions: " + suggestions);
        String json = OBJECT_MAPPER.writeValueAsString(suggestions);
        res.body(json);
        res.type("application/json");
    }
}