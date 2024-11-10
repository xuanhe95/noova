package cis5550.webserver.handler.redirect;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.handler.ResponseHandler;
import cis5550.webserver.Request;

public interface RedirectHandler extends ResponseHandler {
    static void handle(Request req, DynamicResponse res, String url){

    }
}
