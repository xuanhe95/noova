package org.noova.gateway;


import org.noova.gateway.controller.Controller;
import org.noova.gateway.trie.Trie;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.Server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.noova.webserver.Server.get;


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

        String staticFilesPath = PropertyLoader.getProperty("gateway.static.files");
        if (staticFilesPath == null || staticFilesPath.isEmpty()) {
            log.error("Static files location is not set in properties.");
            throw new IllegalArgumentException("Static files location must be configured.");
        }
        Server.staticFiles.location(staticFilesPath);

        Controller.registerRoutes();

        get("/", (req, res) -> {
            res.type("text/html");
            return loadIndexHtml();
        });
    }

    private static String loadIndexHtml() {
        try {
            // Load index.html as a resource from the classpath
            InputStream inputStream = GatewayServer.class.getClassLoader().getResourceAsStream("static/index.html");

            if (inputStream == null) {
                throw new IOException("File not found in classpath: static/index.html");
            }

            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return "<html><body><h1>Error loading page</h1></body></html>";
        }
    }
}
