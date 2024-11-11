package org.noova.webserver.method;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.handler.HandlerStrategy;
import org.noova.webserver.http.HttpStatus;

public class PostHandler implements MethodHandler {

    public static void handle(Request req, DynamicResponse res){
        HandlerStrategy.handle(HttpStatus.METHOD_NOT_ALLOWED, req, res);
    }
}
