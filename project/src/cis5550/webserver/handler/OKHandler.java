package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.FileLocationHelper;
import cis5550.webserver.Request;
import cis5550.webserver.Server;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.io.FileIOStrategy;

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
