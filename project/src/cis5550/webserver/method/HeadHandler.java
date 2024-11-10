package cis5550.webserver.method;

import cis5550.webserver.DynamicResponse;
import cis5550.tools.Logger;
import cis5550.webserver.Request;

public class HeadHandler implements MethodHandler {
    private static final Logger log = Logger.getLogger(GetHandler.class);

    public static void handle(Request req, DynamicResponse res){
        GetHandler.handle(req, res);
        log.info("Removing body from response");
        res.clearBody();
    }
}
