package org.noova.webserver.parser;

import org.noova.tools.Logger;
import org.noova.webserver.DynamicResponse;
import org.noova.webserver.Server;
import org.noova.webserver.handler.HandlerStrategy;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.host.HostManager;
import org.noova.webserver.http.HttpMethod;
import org.noova.webserver.http.HttpStatus;
import org.noova.webserver.http.HttpVersion;
import org.noova.webserver.io.NetworkIOStrategy;
import org.noova.webserver.method.MethodHandlerFactory;
import org.noova.webserver.route.RouteManager;
import org.noova.webserver.session.SessionManager;

import java.io.IOException;


/**
 * @author Xuanhe Zhang
 */
public class RequestParser {

    private static final Logger log = Logger.getLogger(RequestParser.class);

    NetworkIOStrategy io;

    public RequestParser(NetworkIOStrategy io) {
        if(io == null){
            log.error("I/O cannot be null");
            throw new IllegalArgumentException("I/O cannot be null");
        }
        this.io = io;
    }

/*
 * This method will parse the request and keep parsing until the connection is closed or the request is invalid
 */
    public void parse() throws IOException {
        StateMachine state = new StateMachine();
        while(true){
            log.info("Parsing one new request");
            DynamicResponse res = new DynamicResponse(io);
            RawRequest raw = state.parse(io);
            log.info("Raw request parsed");

            // if malformed request, return 400
            if(raw == null){
                // this one changed for hw1.
                log.info("Invalid request, closing connection");
                HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, res);
                res.commit();
                break;
            }

            // if EOF, close connection
            if(!raw.isValid()){
                log.warn("Received EOF, closing connection");
                break;
            }

            // if there is no host header, return 400
            if(!raw.getHeaders().containsKey(HttpHeader.HOST.getHeader())){
                log.warn("No host header, closing connection");
                HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, res);
                res.commit();
                break;
            }

            // process the request
            handle(raw, res);

            if(((DynamicResponse) res).isCloseConnection()){
                log.warn("Response is close connection, closing connection without waiting");
                try{
                    io.closeAll();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
        }
        io.close();

    }

    public void handle(RawRequest raw, DynamicResponse res) throws IOException {

        if(raw.getMethod() == null || raw.getUri() == null || raw.getVersion() == null){
            log.warn("Invalid request");
            HandlerStrategy.handle(HttpStatus.BAD_REQUEST, null, res);
            res.commit();
            return;
        }

        // if the version is not HTTP/1.1, return 505
        // NOTE: HTTP version is case-sensitive
        if(!raw.getVersion().equals( HttpVersion.HTTP_1_1.getVersion())){
            HandlerStrategy.handle(HttpStatus.VERSION_NOT_SUPPORTED, null, res);
            res.commit();
            return;
        }

        // if the method is not GET, HEAD, POST, PUT, return 501
        // NOTE HTTP request method is case-sensitive
        try{
            HttpMethod.valueOf(raw.getMethod());
        } catch(IllegalArgumentException e){
            HandlerStrategy.handle(HttpStatus.NOT_IMPLEMENTED, null, res);
            res.commit();
            return;
        }

        byte[] body = raw.getBody() == null ? "".getBytes() : raw.getBody();
        log.warn("[parse] uri: " + raw.getUri());
        log.info("[parse] Body is: " + new String(body));

        org.noova.webserver.RequestImpl req = new org.noova.webserver.RequestImpl(
                raw.getMethod(),
                raw.getUri(),
                raw.getVersion(),
                raw.getHeaders(),
                RouteManager.parseQueryParams(raw.getUri(), raw.getBody(), raw.getHeaders()),
                RouteManager.getParams(raw.getMethod(), raw.getUri(), raw.getHost()),
                io.getRemoteAddress(),
                 body,
                null
        );


        req.setSecure(io.isSecure());
        log.info("Request is secure: " + req.isSecure());

        //SessionManager.handle(req, res);

        if(!RouteManager.checkAndRoute(req, res)){
            log.info("Static file");

            if(Server.staticFiles.root == null){
                log.error("Static file root is not set");
                HandlerStrategy.handle(HttpStatus.NOT_FOUND, null, res);
                res.commit();
                return;
            }

            MethodHandlerFactory.handle(req, res);


            SessionManager sessionManager = HostManager.getHost(RouteManager.getVirtualHostName(req)).getSessionManager();
            sessionManager.handle(req, res);

            log.info("Response is committed: " + res.isCommitted());
            log.info("Committing last response");
            if(res.isCommitted()){
                return;
            }

            res.commit();
        }

    }


}
