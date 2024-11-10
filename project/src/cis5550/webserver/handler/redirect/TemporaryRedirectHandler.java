package cis5550.webserver.handler.redirect;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Request;
import cis5550.webserver.Server;
import cis5550.webserver.handler.ResponseHandler;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;

public class TemporaryRedirectHandler implements ResponseHandler {
    public static void handle(Request req, DynamicResponse res){
        if(res.getHeader(HttpHeader.LOCATION.getHeader()).isEmpty()) {
            throw new IllegalArgumentException("Location header is required for 302 response");
        }

        String location = res.getHeader(HttpHeader.LOCATION.getHeader()).iterator().next();

        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
        res.setStatusWithBody(HttpStatus.TEMPORARY_REDIRECT, "Redirecting to " + location);
    }
}
