package org.noova.webserver.handler;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

/**
 * @author Xuanhe Zhang
 */
public class MethodNotAllowedHandler implements ResponseHandler {

    public static void handle(Request req, DynamicResponse res){
        res.setStatusWithMessage(HttpStatus.METHOD_NOT_ALLOWED);
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
    }
}
