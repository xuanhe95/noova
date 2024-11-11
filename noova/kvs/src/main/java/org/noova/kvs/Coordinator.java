package org.noova.kvs;

import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;
import org.noova.webserver.Server;

/**
 * @author Xuanhe Zhang
 */
public class Coordinator extends org.noova.generic.Coordinator {

    private static final Logger log = Logger.getLogger(Coordinator.class);
    public static void main(String[] args) {
        String rawPort;
        int port;
        if (args.length < 1) {
            log.error("Usage: Coordinator <port>");
            log.info("[kvs] No arguments provided, using default values");
            rawPort = PropertyLoader.getProperty("kvs.port");
            if(rawPort == null){
                log.error("No port provided, using default port 8000");
                rawPort = "8000";
            }
            port = Integer.parseInt(rawPort);

            // System.exit(1);
        } else{
            port = Integer.parseInt(args[0]);
        }

        try {
            Server.port(port);

            log.info("[kvs] Starting coordinator on port " + port);

            registerRoutes();

            Server.get("/", (req, res) -> {
                return "<!DOCTYPE html><html><head><title>KVS Coordinator</title></head><body>"
                        + "<h1>KVS Coordinator</h1>" + workerTable() + "</body></html>";
            });
        } catch (Exception e) {
            log.error("Error starting coordinator", e);
        }
    }




}
