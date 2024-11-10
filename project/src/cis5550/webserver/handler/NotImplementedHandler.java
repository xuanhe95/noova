package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Server;
import cis5550.webserver.Request;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;

public class NotImplementedHandler implements ResponseHandler {

    public static void handle(Request req, DynamicResponse res){
        res.setStatusWithMessage(HttpStatus.NOT_IMPLEMENTED);
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
    }
}
