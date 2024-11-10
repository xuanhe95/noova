package cis5550.webserver.method;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.handler.HandlerStrategy;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.Request;
import cis5550.webserver.http.HttpMethod;

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
