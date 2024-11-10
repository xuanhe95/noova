package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.FileLocationHelper;
import cis5550.webserver.Server;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class NotModifiedHandler implements ResponseHandler{

    private static final Logger log = Logger.getLogger(NotModifiedHandler.class);

    public static void handle(Request req, DynamicResponse res){

        String location = FileLocationHelper.getLocation(req);

        File file = new File(location);
        if(!file.exists()){
            HandlerStrategy.handle(HttpStatus.NOT_FOUND, req, res);
            return;
        }
        String lastModifiedSince = req.headers(HttpHeader.IF_MODIFIED_SINCE.getHeader());

        if(lastModifiedSince == null){
            HandlerStrategy.handle(HttpStatus.OK, req, res);
            return;
        }

        Instant now = Instant.now();

        try{


            DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(lastModifiedSince, formatter);
            Instant requestInstant = zonedDateTime.toInstant();
            FileTime fileTime = Files.getLastModifiedTime(file.toPath());
            Instant fileInstant = fileTime.toInstant();

            if(requestInstant.isAfter(now)){
                log.info("Request time is in the future");
                HandlerStrategy.handle(HttpStatus.OK, req, res);
                return;
            }

            log.info("Request time: " + requestInstant);
            log.info("File time: " + fileInstant);
            if(fileInstant.isAfter(requestInstant)){
                log.info("File is modified");
                HandlerStrategy.handle(HttpStatus.OK, req, res);
            }else{
                log.info("File is not modified");
                res.status(HttpStatus.NOT_MODIFIED);
                res.header(HttpHeader.CONTENT_LENGTH.getHeader(), "0");
                res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
                res.header(HttpHeader.CONTENT_TYPE.getHeader(), SuffixTool.getContentHeader(req.url()));
            }
        } catch (IOException e){
            HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
        } catch (DateTimeParseException e){
            HandlerStrategy.handle(HttpStatus.OK, req, res);
        }

    }
}
