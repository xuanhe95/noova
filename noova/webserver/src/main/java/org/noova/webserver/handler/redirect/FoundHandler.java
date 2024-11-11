package org.noova.webserver.handler.redirect;

import org.noova.tools.Logger;
import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.handler.ResponseHandler;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

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
