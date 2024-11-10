package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Server;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;

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
