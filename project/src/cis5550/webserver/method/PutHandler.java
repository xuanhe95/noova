package cis5550.webserver.method;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.Request;
import cis5550.webserver.handler.HandlerStrategy;
import cis5550.webserver.http.HttpStatus;

/**
 * @author Xuanhe Zhang
 */
public class PutHandler implements MethodHandler {
    public static void handle(Request req, DynamicResponse res){
        HandlerStrategy.handle(HttpStatus.METHOD_NOT_ALLOWED, req, res);
    }
}
