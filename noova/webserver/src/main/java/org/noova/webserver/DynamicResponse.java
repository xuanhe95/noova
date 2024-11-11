package org.noova.webserver;

import org.noova.tools.Logger;
import org.noova.webserver.handler.HandlerStrategy;
import org.noova.webserver.header.ContentTypeFactory;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.http.HttpStatus;
import org.noova.webserver.io.ByteArrayIOStrategy;
import org.noova.webserver.io.IOStrategy;
import org.noova.webserver.io.NetworkIOStrategy;

import java.io.IOException;
import java.util.*;

/**
 * @author Xuanhe Zhang
 */
public class DynamicResponse implements Response {
    private static final Logger log = Logger.getLogger(DynamicResponse.class);
    private int statusCode = 200;
    private String reasonPhrase = "OK";
    private byte[] body;

    /*
     * The following fields are used to keep track of the state of the response
     */

    // This field is used to keep track of whether the response has been committed
    private boolean committed = false;
    private boolean headerWritten = false;
    public boolean closeConnection = false;

    private final Map<String, List<String>> headers = new HashMap<>();
    private final NetworkIOStrategy io;
    private IOStrategy bodyStream;

    private boolean halted = false;
    private Phase phase = Phase.BEFORE;

    public DynamicResponse(NetworkIOStrategy io){
        this.io = io;
    }



    public List<String> getHeader(String key){
        return headers.getOrDefault(key, new ArrayList<>());
    }


    public void setStatusWithBody(HttpStatus status, String body){
        status(status);
        //header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(body.length()));
        this.body(body);
    }

    public void setStatusWithBody(int statusCode, String body){
        status(statusCode, HttpStatus.valueOf(String.valueOf(statusCode)).getMessage());
        //header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(body.length()));
        this.body(body);
    }

    public void setStatusWithMessage(HttpStatus status){
        status(status);
        //header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(status.getMessage().length()));
        this.body(status.getMessage());
        type(ContentTypeFactory.get("txt").getHeader());
    }


    public void body(IOStrategy bodyStream, int length){
        this.bodyStream = bodyStream;
        header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(length));
    }


    public void clearBody(){
        log.warn("Head req");
        this.body = new byte[0];
        this.bodyStream = new ByteArrayIOStrategy(new byte[0]);
    }


    @Override
    public void body(String body) {
        if(body == null){
            log.warn("Body is null");
            return;
        }
        bodyAsBytes(body.getBytes());
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if(committed){
            log.warn("Response already committed");
            return;
        }
        if(bodyArg == null){
            log.warn("Body is null");
            return;
        }
        this.body = bodyArg;
        this.bodyStream = new ByteArrayIOStrategy(bodyArg);
        log.info("Setting content length to " + bodyArg.length);
        header(HttpHeader.CONTENT_LENGTH.getHeader(), String.valueOf(bodyArg.length));
    }

    @Override
    public void header(String name, String value) {
        List<String> values = headers.getOrDefault(name.toLowerCase(), new ArrayList<>());
        if(name.equalsIgnoreCase(HttpHeader.CONTENT_LENGTH.getHeader())) {
            log.info("Setting content length to " + value);
            values.clear();
        } else if(name.equalsIgnoreCase(HttpHeader.HOST.getHeader())){
            log.info("Setting host to + value");
            values.clear();
        }
        values.add(value);
        headers.put(name.toLowerCase(), values);
    }

    @Override
    public void type(String contentType) {
        headers.getOrDefault(HttpHeader.CONTENT_TYPE.getHeader(), new ArrayList<>()).clear();
        header(HttpHeader.CONTENT_TYPE.getHeader(), contentType);
    }

    public void status(HttpStatus status) {
        //this.status = status;
        this.statusCode = status.getCode();
        this.reasonPhrase = status.getMessage();
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    public boolean isCloseConnection(){
        return closeConnection;
    }


    @Override
    public void write(byte[] b) throws Exception {
        if(committed){
            log.warn("Writing to committed response" + Arrays.toString(b));
            io.write(b);
            return;
        }
        log.info("Writing bytes to response");

        writeStatusLine();

        // this is another way to close the connection, without the content-length header
        header("Connection", "close");
        headers.getOrDefault(HttpHeader.CONTENT_LENGTH.getHeader(), new ArrayList<>()).clear();

        // this means that the parser will break the connection after the response is sent
        closeConnection =true;

        if(!headerWritten){
            log.info("Writing header in write()");
            writeHeader();
        }

        io.write(b);
        log.info("Written" + Arrays.toString(b));
        committed = true;
    }

    public boolean isCommitted() {
        return committed;
    }

    private String statusLine() {
        if(statusCode == 0 || reasonPhrase == null){
            log.warn("Status code or reason phrase not set");
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, this);
            commit();
        }
        log.info("Creating status line");
        return Server.VERSION.getVersion() + " " + statusCode + " " + reasonPhrase + "\r\n";
    }

    private void writeStatusLine(){
        try{
            log.info("Writing status line");
            log.info("Status: " + statusLine());

            io.write(statusLine().getBytes());
            //io.out().write(statusLine().getBytes());
        } catch(Exception e){
            log.error("Error writing status line", e);
        }
    }


    /*
     * This method is used to commit the response. It writes the status line, headers and body to the output stream
     * After committing the response, the response is considered committed and no further changes can be made
     */

    public void commit() {
        if(committed){
            log.warn("Response already committed");
            return;
        }
        log.info("Committing response in commit");

        writeStatusLine();

        try{
            log.info("Writing headers");
            if(bodyStream == null){
                header(HttpHeader.CONTENT_LENGTH.getHeader(), "0");
            }
            writeHeader();
        } catch(Exception e){
            log.error("Error writing headers", e);
        }

        try{
            log.info("Writing body");
            writeBody();
        } catch(Exception e){
            log.error("Error writing body", e);
        }

        committed = true;
    }

    public void finish(){
        log.warn("Finishing response");
        try{
            io.close();
        } catch (IOException e) {
            log.error("Error closing IO", e);
            throw new RuntimeException(e);
        }
    }


    private void writeBody(){
        if(bodyStream == null){
            log.warn("No body to write");
            return;
        }
        byte[] buffer = new byte[4096];
        int bytesRead;
        try{
            while((bytesRead = bodyStream.read(buffer)) != -1){
                io.write(buffer, 0, bytesRead);
            }
        } catch(Exception e){
            log.error("Error writing body", e);
        }
    }

    private void writeHeader(){
        if(headerWritten){
            log.warn("Headers already written");
            return;
        }

        log.info("Writing headers");

        try{

            for(Map.Entry<String, List<String>> header : headers.entrySet()){
                for(String value : header.getValue()){
                    log.info("Writing header: " + header.getKey() + ": " + value);
                    io.write((header.getKey() + ": " + value + "\r\n").getBytes());
                }
            }
            log.info("Writing blank line");
            io.write("\r\n".getBytes());
            headerWritten = true;
        }
        catch(Exception e){
            log.error("Error writing headers", e);
        }
    }

    @Override
    public void redirect(String url, int responseCode) {
        HttpStatus status = null;

        if(responseCode < 300 || responseCode > 399){
            log.error("Invalid status code");
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, this);
            commit();
            return;
        }

        try {
            status = HttpStatus.value(responseCode);
        } catch(IllegalArgumentException e){
            log.error("Invalid status code", e);
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, this);
            commit();
            return;
        }

        if(status == null){
            log.error("Invalid status code");
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, this);
            commit();
            return;
        }

        status(status);
        log.info("Redirecting to " + url);
        header(HttpHeader.LOCATION.getHeader(), url);
        HandlerStrategy.handle(status, null, this);
        commit();
    }



    public void nextPhase(){
        if(phase == Phase.BEFORE){
            phase = Phase.MAIN;
        } else if(phase == Phase.MAIN){
            phase = Phase.AFTER;
        } else {
            log.error("No more phases");
        }
    }

    enum Phase{
        BEFORE,
        MAIN,
        AFTER
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        if(phase != Phase.BEFORE){
            log.error("Halt can only be called in before phase");
            return;
        }
        status(statusCode, reasonPhrase);
        body(reasonPhrase);
        halted = true;
        commit();
    }

    public boolean isHalted(){
        return halted;
    }

}

