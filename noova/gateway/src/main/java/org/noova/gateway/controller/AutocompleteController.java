package org.noova.gateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.SearchService;
import org.noova.tools.Logger;
import org.noova.tools.Parser;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
        String limitParam = "2"; //TBD hardcoded 10
        int limit = Integer.parseInt(limitParam); //TBD hardcoded 10

        if (prefix == null || prefix.isEmpty()) {
            log.warn("[autocomplete] Empty prefix received");
            res.body("[]");
            res.type("application/json");
            return;
        }

        String[] words = prefix.trim().split("\\s+"); // Split by spaces
        String lastWord = words[words.length - 1]; // Get the last word

        log.info("[autocomplete] Last word to autocomplete: " + lastWord);

        // Fetch suggestions for the last word
        List<String> wordSuggestions = SearchService.getInstance().getAutocompleteSuggestions(lastWord, limit);

        // Combine suggestions with the rest of the prefix
        List<String> fullSuggestions = new ArrayList<>();
        String prefixWithoutLastWord = String.join(" ", Arrays.copyOf(words, words.length - 1)).trim();

        for (String suggestion : wordSuggestions) {
            if (!prefixWithoutLastWord.isEmpty()) {
                fullSuggestions.add(prefixWithoutLastWord + " " + suggestion);
            } else {
                fullSuggestions.add(suggestion);
            }
        }

        log.info("[autocomplete] Full suggestions: " + fullSuggestions);

        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(fullSuggestions);
        res.body(json);
        res.type("application/json");
    }



    @Route(path = "/correction", method = "GET")
    private void getCorrection(Request req, Response res) throws IOException {

        String query = req.queryParams("query");
        String limitParam = req.queryParams("limit") == null ? "10" : req.queryParams("limit");
        int limit = Integer.parseInt(limitParam); //TBD hardcoded 10

        if (query == null || query.isEmpty()) {
            log.warn("[autocomplete] Empty prefix received");
            res.body("[]");
            res.type("application/json");
            return;
        }

        List<String> wordTokens = Parser.getLammelizedWords(query);

        String[] words = query.trim().split("\\s+"); // Split by spaces
        String lastWord = words[words.length - 1]; // Get the last word

        List<String> result = SearchService.getInstance().getCorrection(lastWord, limit);




        // Return JSON response
        String json = OBJECT_MAPPER.writeValueAsString(result);
        res.body(json);
        res.type("application/json");
    }
}