package org.noova.webserver.parser;

import org.noova.tools.Logger;
import org.noova.webserver.io.IOStrategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class StateMachine{

    private static final Logger log = Logger.getLogger(StateMachine.class);
    ParserState state;
    StringBuilder line;

    int contentLength = 0;
    boolean hasNext= false;

    String method;
    String uri;
    String version;
    ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
    Map<ParserState, String> values = new HashMap<>();

    Map<String, String> headers = new HashMap<>();

    List<RawRequest> requests = new ArrayList<>();

    private void nextState(){
        switch (state) {
            case STATUS_LINE -> state = ParserState.STATUS_LINE_CR;
            case STATUS_LINE_CR -> state = ParserState.HEADER;
            case HEADER -> state = ParserState.HEADER_CR;
            case HEADER_CR -> state = ParserState.HEADER_END;
            case HEADER_END -> state = ParserState.HEADER_FINISH_CR;
            case HEADER_FINISH_CR -> state = ParserState.FINISHED;
            case BODY -> state = ParserState.FINISHED;
            default -> log.error("Error State");
        }
    }

    private void errorState(){
        state = ParserState.ERROR;
    }

    private void parseLine(){
        log.info("Added: " + state + " " + line.toString());
        values.put(state, line.toString());
        line.setLength(0);
        nextState();
    }

    private void parseHeader(){
        if(line.isEmpty()){
            log.info("Empty Header");
            state = ParserState.HEADER_FINISH_CR;
            return;
        }
        String[] words = line.toString().split(":",2);
        if(words.length != 2){

            log.error("Wrong Header Length: " + words.length);
            errorState();
        } else {
            if("content-length".equals(words[0].toLowerCase().strip())){
                log.info("Content-Length: " + words[1]);
                try{
                    contentLength = Integer.parseInt(words[1].strip());
                } catch (NumberFormatException e){
                    log.error("Content-Length is not a number");
                    errorState();
                }
                hasNext = true;
            }

            // NOTE: headers' keys are stored in lowercase

            headers.put(words[0].toLowerCase().strip(), words[1].strip());
            log.info("Header: " + words[0].toLowerCase().strip());
            log.info("Value: " + words[1].strip());
        }
        line.setLength(0);
        nextState();
    }

    private void checkIfBody(){
        if(hasNext && contentLength > 0){
            log.info("parse body");
            state = ParserState.BODY;
        } else{
            log.info("No body");
            state = ParserState.FINISHED;
        }
    }


    private void parseBody(){
        log.info("Content-Length: " + contentLength);
        if(hasNext && contentLength == 1){
            // parseLine();
            //body = line.toString();
            state = ParserState.FINISHED;


            // since one connection can have multiple requests, we need to reset the content length
            // this is a bug in the original code
            contentLength = 0;
        }
        else if(contentLength > 0){
            contentLength--;
        }
    }

    public RawRequest parse(IOStrategy io){
        line = new StringBuilder();
        headers = new HashMap<>();
        state = ParserState.STATUS_LINE;
        long start = System.currentTimeMillis();
        while(true){
            try {

                int b = io.read();
                if(state == ParserState.ERROR){
                    log.warn("Error State, return null");
                    return null;
                }

                // if EOF
                if(b == -1){
                    // if the state line is not finished and the current line is empty, which means this request is just started
                    // return INVALID raw request, which will end the connection without error
                    // TODO: This cloud have a better way to handle in the future

                    if(state == ParserState.STATUS_LINE && line.isEmpty()){
                        log.info("No more bytes");
                        RawRequest request = new RawRequest();
                        request.setValid(false);
                        return request;
                    }

                    // if the state is not finished or body, which means the request is not finished yet
                    // return null, which will end the connection with 400 error

                    if(state != ParserState.FINISHED && state != ParserState.BODY){
                        log.error("State: " + state);
                        log.error("Unexpected end of stream");
                        return null;
                    }

                    // if state is body and content-length is not 0, which means the body is not finished yet
                    // return null, which will end the connection with 400 error

                    if(state == ParserState.BODY && contentLength > 0){
                        log.error("Unexpected end of stream");
                        return null;
                    }



                    String statusLine = values.get(ParserState.STATUS_LINE);


                    log.info("Body: " + bodyStream.toString());
                    log.info("Time Finished: " + (System.currentTimeMillis() - start) + "ms");
                    log.info("Method: " + method);
                    log.info("URI: " + uri);
                    log.info("Version: " + version);

                    RawRequest request = new RawRequest(method, uri, version, headers, bodyStream.toByteArray());
                    bodyStream.reset();
                    log.info("Content-Length: " + contentLength);

                    return request;
                }
                next((char) b);
                if(state == ParserState.FINISHED) {
                    log.info("Time Finished: " + (System.currentTimeMillis() - start) + "ms");
                    RawRequest request = new RawRequest(method, uri, version, headers, bodyStream.toByteArray());
                    bodyStream.reset();
                    if(headers.get("content-length") == null){
                        log.warn("No content-length header");
                    }
                    return request;
                }
            } catch(SocketException e){
                log.error("Socket closed");
                RawRequest request = new RawRequest();
                request.setValid(false);
                return request;
            } catch (IOException e){
                log.error("Error reading from stream");
                log.error(e.getMessage());
                return null;
            }
        }
    }

    private void parseStatusLine(){
        String[] words = line.toString().split(" +");
        if(words.length != 3){
            log.error("Wrong Status Line Length: " + words.length);
            errorState();
        } else {

            method = words[0];
            uri = words[1];
            version = words[2];

            log.info("Method: " + words[0]);
            log.info("URI: " + words[1]);
            log.info("Version: " + words[2]);
        }
        line.setLength(0);
        nextState();
    }


    public void parse(byte[] buffer, int readBytes){

    }
    private void next(char c){
        log.debug("Byte: " + (byte) c + "    Char: " + c);
        log.debug("Original State: " + state);
        switch (state){
                case STATUS_LINE:
                switch(c){
                    case '\r' -> nextState();
                    default -> line.append(c);
                }
                break;
            case STATUS_LINE_CR:
                switch(c){
                    case '\n' -> parseStatusLine();
                    default -> errorState();
                }
                break;
            case HEADER:
                switch(c){
                    case '\r' -> parseHeader();
                    default -> line.append(c);
                }
                break;
            case HEADER_CR:
                switch(c){
                    case '\n' -> nextState();
                    default -> errorState();
                }
                break;
            case HEADER_END:
                switch(c){
                    case '\r' -> state = ParserState.HEADER_FINISH_CR;
                    case '\n' -> errorState();
                   // default -> state = ParserState.HEADER;
                    default -> {
                        line.append(c);
                        state = ParserState.HEADER;
                    }
                }
                break;
            case HEADER_FINISH_CR:
                switch(c){
                    case '\n' -> checkIfBody();
                    default -> errorState();
                }
                break;
            case BODY:
                bodyStream.write(c);
                parseBody();
                break;
        }

        log.debug("State: " + state);

    }



}