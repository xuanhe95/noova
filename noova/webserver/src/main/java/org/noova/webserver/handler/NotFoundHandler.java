package org.noova.webserver.handler;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

public class NotFoundHandler implements ResponseHandler {

    public static void handle(Request req, DynamicResponse res){
        res.setStatusWithMessage(HttpStatus.NOT_FOUND);
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
    }
}