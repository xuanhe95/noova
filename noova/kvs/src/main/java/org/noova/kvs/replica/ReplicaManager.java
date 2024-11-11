package org.noova.kvs.replica;

import org.noova.generic.node.Node;
import org.noova.tools.HTTP;
import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Route;
import org.noova.webserver.Server;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ReplicaManager {

    private static final Logger log = Logger.getLogger(ReplicaManager.class);
    public static final String REPLICA_PATH = "/replica";
    public static final int REPLICA_NUM = 2;

    public static void put(String uri, Route route){
        ForwardingRouteDecorator forwardingRoute = new ForwardingRouteDecorator(route);
        Server.put(REPLICA_PATH + uri, route);
        Server.put(uri, forwardingRoute);
    }

    static String getReplicaUri(Request req){
        Set<String> queryParams = req.queryParams();
        StringBuilder builder = new StringBuilder(REPLICA_PATH + req.url());

        log.warn("[replica] url: " + req.url());
        if (!queryParams.isEmpty()) {
            boolean firstParam = true;
            for (String param : queryParams) {
                if (firstParam) {
                    builder.append("?");
                    firstParam = false;
                } else {
                    builder.append("&");
                }
                builder.append(param);
                builder.append("=");
                builder.append(req.queryParams(param));
            }
        }
        log.info("[replica] Replica URI: " + builder.toString());
        return builder.toString();
    }

    public static void forwardReplicas(List<Node> replicas, String uri, byte[] data){
            for (Node node : replicas) {
                Thread thread = new Thread(() -> {
                    try {
                        log.info("[replica] Forwarding to replica: " + node.asIpPort());
                        HTTP.Response res = HTTP.doRequest("PUT", "http://" + node.asIpPort() + uri, data);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                thread.start();
            }

    }


//    public static boolean request(String addr, String uri, String method, byte[] body){
//        try {
//            URL url = URI.create("http://" + addr + uri).toURL();
//
//
//            log.info("Requesting: " + url);
//
//            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod(method);
//            // set the body
//            conn.setDoOutput(true);
//
//            if(body != null){
//                conn.getOutputStream().write(body);
//            }
//
//            int responseCode = conn.getResponseCode();
//            if(responseCode != HttpStatus.OK.getCode()){
//                log.error("Error requesting: " + responseCode);
//                return false;
//            }
//            return true;
//        } catch (Exception e) {
//            log.error("Error requesting", e);
//            return false;
//        }
//    }







}
