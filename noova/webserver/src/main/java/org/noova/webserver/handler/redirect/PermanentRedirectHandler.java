package org.noova.webserver.handler.redirect;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.handler.ResponseHandler;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

public class PermanentRedirectHandler implements ResponseHandler {
    public static void handle(Request req, DynamicResponse res) {
        if (res.getHeader(HttpHeader.LOCATION.getHeader()).isEmpty()) {
            throw new IllegalArgumentException("Location header is required for 301 response");
        }

        String location = res.getHeader(HttpHeader.LOCATION.getHeader()).iterator().next();

        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
        res.setStatusWithBody(HttpStatus.PERMANENT_REDIRECT, "Redirecting to " + location);
    }
}
