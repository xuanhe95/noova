package cis5550.kvs;

import cis5550.tools.Logger;
import cis5550.webserver.Server;

/**
 * @author Xuanhe Zhang
 */
public class Coordinator extends cis5550.generic.Coordinator {

    private static final Logger log = Logger.getLogger(Coordinator.class);
    public static void main(String[] args) {
        if (args.length != 1) {
            log.error("Usage: Coordinator <port>");
            System.exit(1);
        }

        try {
            int port = Integer.parseInt(args[0]);
            Server.port(port);

            log.info("Starting coordinator on port " + port);

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
