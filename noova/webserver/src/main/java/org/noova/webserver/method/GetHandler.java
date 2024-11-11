package org.noova.webserver.method;

import org.noova.tools.Logger;
import org.noova.webserver.DynamicResponse;
import org.noova.webserver.FileLocationHelper;
import org.noova.webserver.Request;
import org.noova.webserver.handler.HandlerStrategy;
import org.noova.webserver.header.ContentTypeFactory;
import org.noova.webserver.header.ContentTypeHeader;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

import java.io.File;

/**
 * @author Xander Zhang
 */
public class GetHandler implements MethodHandler {

    private static final Logger log = Logger.getLogger(GetHandler.class);


    public static void handle(Request req, DynamicResponse res){


        String path = FileLocationHelper.getLocation(req);
        String suffix = path.substring(path.lastIndexOf(".") + 1);
        ContentTypeHeader contentType = ContentTypeFactory.get(suffix);

        log.info("GET request for " + path);

        // security check
        if(path.contains("..")){
            HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
            return;
        }

        log.info(System.getProperty("user.dir"));

        File file = new File(path);

        log.info(file.getAbsolutePath());

        // file not found
        if(!file.exists()){
            log.info("File not found");
            HandlerStrategy.handle(HttpStatus.NOT_FOUND, req, res);
            return;
        }

        // file cannot be read
        if(!file.canRead()){
            log.info("File cannot be read");
            HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
            return;
        }

        String ifModifiedSince = req.headers(HttpHeader.IF_MODIFIED_SINCE.getHeader());

        if(ifModifiedSince != null){
            log.info("if-modified-since request");
            HandlerStrategy.handle(HttpStatus.NOT_MODIFIED, req, res);
            return;
        }

        String range = req.headers(HttpHeader.RANGE.getHeader());

        if(range != null){
            log.info("range request");
            HandlerStrategy.handle(HttpStatus.PARTIAL_CONTENT, req, res);
            return;
        }

        HandlerStrategy.handle(HttpStatus.OK, req, res);
    }

}
