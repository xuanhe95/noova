package cis5550.webserver.method;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.FileLocationHelper;
import cis5550.webserver.Request;
import cis5550.webserver.handler.HandlerStrategy;
import cis5550.webserver.header.ContentTypeFactory;
import cis5550.webserver.header.ContentTypeHeader;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;
import cis5550.tools.Logger;

import java.io.*;

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
