package org.noova.gateway;


import org.noova.gateway.controller.Controller;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.Server;


public class GatewayServer {

    private static final Logger log = Logger.getLogger(GatewayServer.class);
    public static String coordinatorAddr;

    public static int port = 8080;
    public static void main(String[] args) {
        System.out.println("Hello, World!");
        if(args.length < 2){
            System.err.println("Syntax: ProxyServer <kvsAddr> <port>");
            log.warn("[gateway] No arguments provided, using default values");
            coordinatorAddr = PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port");
            String rawPort = PropertyLoader.getProperty("gateway.port");
            if(rawPort != null){
                port = Integer.parseInt(rawPort);
            }
        } else{
            coordinatorAddr = args[0];
            port = Integer.parseInt(args[1]);
        }

        log.info("[proxy] ProxyServer starting on port " + port);
        Server.port(port);
        Controller.registerRoutes();

    }
}
