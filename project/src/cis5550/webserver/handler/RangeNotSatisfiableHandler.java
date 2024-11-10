package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.FileLocationHelper;
import cis5550.webserver.Server;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.Request;
import cis5550.webserver.header.HttpHeader;

import java.io.File;

/**
 * @author Xuanhe Zhang
 */
public class RangeNotSatisfiableHandler implements ResponseHandler{

    public static void handle(Request req, DynamicResponse res){
        String location = FileLocationHelper.getLocation(req);

        File file = new File(location);
        res.setStatusWithMessage(HttpStatus.RANGE_NOT_SATISFIABLE);
        res.header(HttpHeader.CONTENT_RANGE.getHeader(), "bytes */" + file.length());
        res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
        res.header(HttpHeader.CONTENT_TYPE.getHeader(), SuffixTool.getContentHeader(req.url()));
    }

}
