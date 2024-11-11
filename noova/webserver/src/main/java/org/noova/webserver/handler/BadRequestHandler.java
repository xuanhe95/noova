package org.noova.webserver.handler;

import org.noova.tools.Logger;
import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

/**
 * @author Xuanhe Zhang
 */
public class BadRequestHandler implements ResponseHandler {

    private static final Logger log = Logger.getLogger(BadRequestHandler.class);
    public static void handle(Request req, DynamicResponse res){
        log.info("Handle bad request");
        res.setStatusWithMessage(HttpStatus.BAD_REQUEST);
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
    }

}
