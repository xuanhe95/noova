package cis5550.webserver.route;

import cis5550.webserver.Route;
import cis5550.webserver.http.HttpMethod;

import java.util.Map;

public interface Router {

    void addRoute(HttpMethod method, String path, cis5550.webserver.Route route);

    Route getRoute(HttpMethod method, String path);


//    void addDynamicRoute(String url, Route route);
//
//    void addDynamicRoute(String url, String routePath);

    Map<String, String> parseParams(String method, String url);

}
