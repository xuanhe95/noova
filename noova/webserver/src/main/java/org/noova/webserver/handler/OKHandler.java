package org.noova.webserver.handler;

import org.noova.webserver.DynamicResponse;
import org.noova.webserver.FileLocationHelper;
import org.noova.webserver.Request;
import org.noova.webserver.Server;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;
import org.noova.webserver.io.FileIOStrategy;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author Xuanhe Zhang
 */
public class OKHandler implements ResponseHandler{

    public static void handle(Request req, DynamicResponse res) {
        String location = FileLocationHelper.getLocation(req);
        File file = new File(location);

        if(!file.exists()){
            HandlerStrategy.handle(HttpStatus.NOT_FOUND, req, res);
            return;
        }

        if(!file.canRead()){
            HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
            return;
        }

        try {
            res.status(HttpStatus.OK);
            res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
            res.header(HttpHeader.CONTENT_TYPE.getHeader(), SuffixTool.getContentHeader(req.url()));
            res.header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(file.length()));
            res.body(new FileIOStrategy(file), (int) file.length());
        } catch (FileNotFoundException e) {
            HandlerStrategy.handle(HttpStatus.NOT_FOUND, req, res);
        }

    }


}
