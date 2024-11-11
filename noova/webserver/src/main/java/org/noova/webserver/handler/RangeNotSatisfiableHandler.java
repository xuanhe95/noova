package org.noova.webserver.handler;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.FileLocationHelper;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;

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
