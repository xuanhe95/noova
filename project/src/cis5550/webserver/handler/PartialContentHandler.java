package cis5550.webserver.handler;

import cis5550.webserver.DynamicResponse;
import cis5550.webserver.FileLocationHelper;
import cis5550.webserver.Server;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.header.HttpHeader;
import cis5550.webserver.http.HttpStatus;
import cis5550.webserver.io.ByteArrayIOStrategy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author Xuanhe Zhang
 */
public class PartialContentHandler implements ResponseHandler {
    private static final Logger log = Logger.getLogger(PartialContentHandler.class);

    public static void handle(Request req, DynamicResponse res){
        String range = req.headers(HttpHeader.RANGE.getHeader());

        if(range == null){
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, req, res);
            return;
        }

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

        long fileLength = file.length();

        log.info("File length: " + fileLength);

        String[] ranges = range.split("=");

        if(ranges.length != 2 || !"bytes".equals(ranges[0].strip()) || !ranges[1].contains("-")){
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, req, res);
            return;
        }

        String[] startEnd = ranges[1].split("-");
        if(startEnd.length != 1 && startEnd.length != 2){
            log.error("Invalid range header");
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, req, res);
            return;
        }

        int start;
        int end;
        try{
            start = startEnd[0].isBlank() ? 0 : Integer.parseInt(startEnd[0].strip());
            end = startEnd.length == 1 ? (int) file.length()-1 : Integer.parseInt(startEnd[1].strip());
        } catch(NumberFormatException e){
            log.error("Invalid range header");
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, req, res);
            return;
        }

        if(start > end || start < 0 || end >= fileLength){
            log.error("Range out of bounds");
            HandlerStrategy.handle(HttpStatus.RANGE_NOT_SATISFIABLE, req, res);
            return;
        }

        int length = end - start + 1;

        log.info("Start: " + start);
        log.info("End: " + end);
        log.info("Length: " + length);

        try(FileInputStream fileInputStream = new FileInputStream(file)) {

            long skippedBytes = fileInputStream.skip(start);

            if(skippedBytes != start){
                log.info("Skipped bytes: " + skippedBytes);
                HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
                return;
            }

            byte[] buffer = new byte[length];

            int bytesRead = fileInputStream.read(buffer, 0, length);

            if(bytesRead == -1){
                log.info("EOF reached");
                HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
                return;
            }


            res.status(HttpStatus.PARTIAL_CONTENT);
            res.header(HttpHeader.CONTENT_TYPE.getHeader(), SuffixTool.getContentHeader(req.url()));
            res.header(HttpHeader.SERVER.getHeader(), Server.SERVER_NAME);
            res.header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(length));
            res.header(HttpHeader.CONTENT_RANGE.getHeader(), "bytes " + start + "-" + end + "/" + file.length());

            res.body(new ByteArrayIOStrategy(buffer), length);
        }catch (FileNotFoundException e){
            HandlerStrategy.handle(HttpStatus.NOT_FOUND, req, res);
        }catch(IOException e){
            HandlerStrategy.handle(HttpStatus.FORBIDDEN, req, res);
        }
    }


}
