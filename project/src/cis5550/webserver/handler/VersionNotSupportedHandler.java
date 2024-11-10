package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Server;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.Request;
import cis5550.webserver.header.HttpHeader;

public class VersionNotSupportedHandler implements ResponseHandler {

    public static void handle(Request req, DynamicResponse res){
        res.setStatusWithMessage(HttpStatus.VERSION_NOT_SUPPORTED);
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
    }
}
