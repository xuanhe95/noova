package org.noova.webserver.handler.redirect;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.handler.ResponseHandler;

public interface RedirectHandler extends ResponseHandler {
    static void handle(Request req, DynamicResponse res, String url){

    }
}
