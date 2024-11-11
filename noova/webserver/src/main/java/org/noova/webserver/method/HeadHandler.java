package org.noova.webserver.method;

import org.noova.tools.Logger;
import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;

public class HeadHandler implements MethodHandler {
    private static final Logger log = Logger.getLogger(GetHandler.class);

    public static void handle(Request req, DynamicResponse res){
        GetHandler.handle(req, res);
        log.info("Removing body from response");
        res.clearBody();
    }
}
