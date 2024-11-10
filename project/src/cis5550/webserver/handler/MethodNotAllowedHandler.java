package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Server;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.Request;
import cis5550.webserver.header.HttpHeader;

/**
 * @author Xuanhe Zhang
 */
public class MethodNotAllowedHandler implements ResponseHandler {

    public static void handle(Request req, DynamicResponse res){
        res.setStatusWithMessage(HttpStatus.METHOD_NOT_ALLOWED);
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
    }
}
