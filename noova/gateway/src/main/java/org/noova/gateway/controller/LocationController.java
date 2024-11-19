package org.noova.gateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.service.LocationService;
import org.noova.gateway.service.SearchService;
import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocationController implements IController {

    private static final Logger log = Logger.getLogger(LocationController.class);

    private static LocationController instance;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
}