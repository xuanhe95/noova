package org.noova.webserver.method;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.handler.HandlerStrategy;
import org.noova.webserver.http.HttpMethod;
import org.noova.webserver.http.HttpStatus;

/**
 * @author Xuanhe Zhang
 */
public class MethodHandlerFactory {
    public static void handle(Request req, DynamicResponse res){
        HttpMethod method;

        try {
            method = HttpMethod.valueOf(req.requestMethod());
        } catch (IllegalArgumentException e) {
            HandlerStrategy.handle(HttpStatus.NOT_IMPLEMENTED, req, res);
            throw new IllegalArgumentException("Unsupported method: " + req.requestMethod());
        }

        switch (method) {
            case GET -> GetHandler.handle(req, res);
            case POST -> PostHandler.handle(req, res);
            case HEAD -> HeadHandler.handle(req, res);
            case PUT -> PutHandler.handle(req, res);
            default -> throw new IllegalArgumentException("Unsupported method: " + req.requestMethod());
        };
    }
}
