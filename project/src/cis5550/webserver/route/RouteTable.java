package cis5550.webserver.route;

import cis5550.webserver.Route;
import cis5550.tools.Logger;
import cis5550.webserver.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;


public class RouteTable implements Router {

    private static final Logger log = Logger.getLogger(RouteTable.class);
    private static final String INTERVAL = "___";

    private final Map<String, cis5550.webserver.Route> routes = new HashMap<>();

    private final Map<String, cis5550.webserver.Route> dynamicRoutes = new HashMap<>();


    private static String getSignature(String method, String path) {
        return method + INTERVAL + path;
    }

    private static String getSignature(HttpMethod method, String path) {
        return method.toString() + INTERVAL + path;
    }



    public void addRoute(HttpMethod method, String path, cis5550.webserver.Route route) {
        String signature = getSignature(method, path);
        log.info("Adding route: " + signature);
        routes.put(signature, route);
    }

    public Route getRoute(HttpMethod method, String path) {


        String signature = getSignature(method, path);

        log.info("Getting route: " + signature);

        if(dynamicRoutes.containsKey(signature)){
            log.info("Getting dynamic route: " + signature);
            return dynamicRoutes.get(signature);
        }


        if(routes.containsKey(signature)) {
            log.info("Found route: " + signature);
            return routes.get(signature);
        }

        log.warn("Route not found: " + signature);
        return null;
    }

//    public void addDynamicRoute(String url, Route route) {
//        dynamicRoutes.put(url, route);
//    }
//
//    public void addDynamicRoute(String url, String routePath) {
//        dynamicRoutes.put(url, routes.get(routePath));
//    }

    public void addDynamicRoute(String url, String routePath) {
        dynamicRoutes.put(url, routes.get(routePath));
    }



    private boolean ifMatchThenAddToRoutes(String method, String url, String routePath) {
        String signature = getSignature(method, url);



        String[] urlParts = url.split("/");


        // This is a crucial bug fix
        String routeMethod = routePath.substring(0, routePath.indexOf(INTERVAL));
        if (!routeMethod.equals(method)) {
            return false;
        }

        // remove method from routePath
        String pureRoutePath = routePath.substring(routePath.indexOf(INTERVAL) + 3);
        String[] routeParts = pureRoutePath.split("/");


        if (urlParts.length != routeParts.length) {
            return false;
        }

        for (int i = 0; i < urlParts.length; i++) {
            if (!isParam(routeParts[i]) && !urlParts[i].equals(routeParts[i])) {
                return false;
            }
        }

        // Dynamic url -> routePath

        log.info("Adding dynamic route: " + signature);
        log.info("Route path: " + routePath);

        dynamicRoutes.put(signature, routes.get(routePath));

        return true;
    }

    private static boolean isParam(String part) {
        if (part.length() == 1) {
            return false;
        }
        return part.startsWith(":");
    }

    @Override
    public Map<String, String> parseParams(String method, String url) {
        log.info("Parsing parameters");


        // This is a curcial bug fix, for parsing params
        String pureUrl = url.split("\\?")[0];


        Map<String, String> params = new HashMap<>();
        for (String key : routes.keySet()) {
            if (ifMatchThenAddToRoutes(method, pureUrl, key)) {
                log.info("Found route: " + key);
                parseParams(pureUrl, key, params);
            }
        }
        return params;
    }

    public static Map<String, String> parseParams(String url, String routePath, Map<String, String> params) {
        log.info("[params] Parsing parameters");
        String[] urlParts = url.split("/");
        String pureRoutePath = routePath.substring(routePath.indexOf(INTERVAL) + 3);
        String[] routeParts = pureRoutePath.split("/");

        if (urlParts.length != routeParts.length) {
            throw new RuntimeException("Invalid number of parameters");
        }


        for (int i = 0; i < urlParts.length; i++) {
            if (isParam(routeParts[i])) {
                log.info("[params] Found parameter: " + routeParts[i] + " with value " + urlParts[i]);
                params.put(routeParts[i].substring(1), urlParts[i]);
                continue;
            } else if (!urlParts[i].equals(routeParts[i])) {
                log.info("[params] Invalid parameter: " + routeParts[i] + " with value " + urlParts[i]);
                throw new RuntimeException("Invalid parameter");
            }
        }
        return params;

    }








}
