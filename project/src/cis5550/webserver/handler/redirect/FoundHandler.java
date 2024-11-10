package cis5550.webserver.handler.redirect;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Server;
import cis5550.webserver.handler.ResponseHandler;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;
import cis5550.tools.Logger;
import cis5550.webserver.Request;

public class FoundHandler implements ResponseHandler {

    private static final Logger log = Logger.getLogger(FoundHandler.class);
    public static void handle(Request req, DynamicResponse res) {
        if(res.getHeader(HttpHeader.LOCATION.getHeader()).isEmpty()) {
            log.error("Location header is required for 302 response");
            throw new IllegalArgumentException("Location header is required for 302 response");
        }

        String location = res.getHeader(HttpHeader.LOCATION.getHeader()).iterator().next();


        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
        res.setStatusWithBody(HttpStatus.FOUND, "Redirecting to " + location);
    }


}
