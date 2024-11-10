package cis5550.webserver.route;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Request;
import cis5550.webserver.filter.FilterManager;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpMethod;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.session.SessionManager;
import cis5550.tools.Logger;
import cis5550.webserver.host.Host;
import cis5550.webserver.host.HostManager;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xuanhe Zhang
 *
 * The route manager is responsible for managing the routers and getting the routes
 */
public class RouteManager {

    private static final Logger log = Logger.getLogger(RouteManager.class);
    private static final Map<Host, Router> ROUTER_TABLES = new HashMap<>();

    public static void addRoute(HttpMethod method, String path, cis5550.webserver.Route route, Host host) {
        Router router = ROUTER_TABLES.getOrDefault(host, new RouteTable());
        router.addRoute(method, path, route);
        ROUTER_TABLES.put(host, router);
    }

    public static String getVirtualHostName(cis5550.webserver.Request req){
        String rawHost = req.headers(HttpHeader.HOST.getHeader());
        if(rawHost == null){
            return null;
        }
        return rawHost.split(":")[0].strip();
    }


    public static Map<String, String> getParams(String method, String url, String hostName){
        log.info("Parsing parameters");

        Host host = HostManager.getHost(hostName);
        log.info("Host Name: " + host.getHostName());
        Router router = ROUTER_TABLES.get(host);

        return router.parseParams(method, url);
    }

//    public static Map<String, String> getParams(String url) {
//        return getParams(url, null);
//    }

    private static String getPureQueryPath(String path){
        int questionMarkIndex = path.indexOf("?");
        int lastSlashIndex = path.lastIndexOf("/");

        if(questionMarkIndex != -1 && lastSlashIndex > questionMarkIndex){
            log.error("Query parameters in path");
            throw new RuntimeException("Invalid path");
        }

        if(questionMarkIndex != -1){
            return path.substring(0, questionMarkIndex);
        }
        return path;
    }

    private static cis5550.webserver.Route getRoute(cis5550.webserver.Request req, cis5550.webserver.Response res) {
        log.info("Checking route");

        // check virtual host

        HttpMethod method = HttpMethod.valueOf(req.requestMethod());
        String path = getPureQueryPath(req.url());

        String hostName = getVirtualHostName(req);
        Host host = HostManager.getHost(hostName);
        Router router = ROUTER_TABLES.get(host);

        return router.getRoute(method, path);
    }


    public static boolean checkAndRoute(cis5550.webserver.Request req, cis5550.webserver.Response res) {
        cis5550.webserver.Route route = getRoute(req, res);
        if (route == null) {
            log.warn("[route manager] No route found");
            return false;
        }
        log.warn("[route manager] Route found");
        route(req, res, route);
        return true;
    }

    private static void route(Request req, cis5550.webserver.Response res, cis5550.webserver.Route route) {

        FilterManager.executeBeforeFilters(req, res);

        ((DynamicResponse) res).nextPhase();



        if(((DynamicResponse)res).isHalted()){
            log.error("[route manager] Request halted");
            return;
        }

        try {
            // set default content type
            res.type("text/html");

            log.info("[route manager] Handling route " + req.requestMethod() + " " + req.url());
            log.info("[route manager] Request body: " + req.body());
            Object data = route.handle(req, res);

            log.info("[route manager] Handling session");
            SessionManager sessionManager = HostManager.getHost(getVirtualHostName(req)).getSessionManager();
            sessionManager.handle(req, res);


            if (data != null) {
                log.info("[route manager] Received data from route: " + data.toString());
                res.body(data.toString());
                res.header("Content-Length", String.valueOf(data.toString().length()));
            } else {
                log.warn("[route manager] No data received from route");
            }

            //res.status(HttpStatus.OK.getCode(), HttpStatus.OK.getMessage());
            res.header("Server", cis5550.webserver.Server.SERVER_NAME);
            //res.header("Content-Type", "text/html");
        } catch (Exception e) {

            if(((DynamicResponse) res).isCommitted()){
                log.error("[route manager] Response already committed when Exception thrown");
                ((DynamicResponse) res).finish();
                return;
            }


            log.error("[route manager] Error handling route " + req.requestMethod() + " " + req.url());
            log.error(e.getMessage());
            res.status(HttpStatus.INTERNAL_SERVER_ERROR.getCode(), HttpStatus.INTERNAL_SERVER_ERROR.getMessage());
            res.header("Server", cis5550.webserver.Server.SERVER_NAME);
            res.header("Content-Length", "0");
        }



        ((DynamicResponse) res).nextPhase();
        FilterManager.executeAfterFilters(req, res);



        ((DynamicResponse) res).commit();

    }

    public static Map<String, String> parseQueryParams(String url, byte[] body, Map<String, String> headers) {
        Map<String, String> queries = new HashMap<>();

        log.info("[query params] Parsing query parameters " + url);
        String[] parts = url.split("\\?");

        if (parts.length != 2) {
            log.warn("[query params] No query mark found, returning empty map");
            return queries;
        }

        String query = parts[1];

        parseQueryParams(query, queries);

        log.info("[query params] Contains Content Type: " + headers.containsKey("content-type"));

        String contentType = headers.getOrDefault(HttpHeader.CONTENT_TYPE.getHeader(), null);

        if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
            log.info("[query params] Parsing body parameters" + new String(body));
            parseQueryParams(new String(body), queries);
        }

        return queries;
    }

    public static void parseQueryParams(String query, Map<String, String> queries) {
        String[] pairs = query.split("&");

        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
//            if (keyValue.length < 2) {
//                log.error("Invalid query parameter");
//                continue;
//                //throw new RuntimeException("Invalid query parameter");
//            }

            String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
            String value = keyValue.length > 1 ? URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8) : "";

            log.info("[query params] Found query parameter: " + key + " with value " + value);
            queries.put(key, value);
        }
    }

}
