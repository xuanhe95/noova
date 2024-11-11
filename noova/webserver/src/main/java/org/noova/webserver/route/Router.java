package org.noova.webserver.route;

import org.noova.webserver.Route;
import org.noova.webserver.http.HttpMethod;

import java.util.Map;

public interface Router {

    void addRoute(HttpMethod method, String path, org.noova.webserver.Route route);

    Route getRoute(HttpMethod method, String path);


//    void addDynamicRoute(String url, Route route);
//
//    void addDynamicRoute(String url, String routePath);

    Map<String, String> parseParams(String method, String url);

}
