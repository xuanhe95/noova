package org.noova.webserver.handler;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Request;
import org.noova.webserver.handler.redirect.*;
import org.noova.webserver.http.HttpStatus;


/**
 * @author Xuanhe Zhang
 */
public class HandlerStrategy {

    public static void handle(HttpStatus status, Request req, DynamicResponse res){
        switch (status) {
            // 200
            case OK -> OKHandler.handle(req, res);
            // 206
            case PARTIAL_CONTENT -> PartialContentHandler.handle(req, res);
            // 301
            case MOVED_PERMANENTLY -> MovedPermanentlyHandler.handle(req, res);
            // 302
            case FOUND -> FoundHandler.handle(req, res);
            // 303
            case SEE_OTHER -> SeeOtherHandler.handle(req, res);
            // 304
            case NOT_MODIFIED -> NotModifiedHandler.handle(req, res);
            // 307
            case TEMPORARY_REDIRECT -> TemporaryRedirectHandler.handle(req, res);
            // 308
            case PERMANENT_REDIRECT -> PermanentRedirectHandler.handle(req, res);
            // 400
            case BAD_REQUEST -> BadRequestHandler.handle(req, res);
            // 403
            case FORBIDDEN -> ForbiddenHandler.handle(req, res);
            // 404
            case NOT_FOUND -> NotFoundHandler.handle(req, res);
            // 405
            case METHOD_NOT_ALLOWED -> MethodNotAllowedHandler.handle(req, res);
            // 416
            case RANGE_NOT_SATISFIABLE -> RangeNotSatisfiableHandler.handle(req, res);
            // 500
            case INTERNAL_SERVER_ERROR -> InternalServerErrorHandler.handle(req, res);
            // 501
            case NOT_IMPLEMENTED -> NotImplementedHandler.handle(req, res);
            // 505
            case VERSION_NOT_SUPPORTED -> VersionNotSupportedHandler.handle(req, res);
            default -> throw new IllegalArgumentException("Invalid status code");
        }

    }
}
