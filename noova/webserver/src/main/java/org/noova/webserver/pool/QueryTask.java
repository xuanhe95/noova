package org.noova.webserver.pool;

import org.noova.tools.Logger;
import org.noova.webserver.io.NetworkIOStrategy;
import org.noova.webserver.parser.RequestParser;

import java.io.IOException;

/**
 * @author Xuanhe Zhang
 */
public class QueryTask implements Runnable {

    private static final Logger log = Logger.getLogger(QueryTask.class);
    private final NetworkIOStrategy io;
    public QueryTask(NetworkIOStrategy io){
        log.info("RequestTask created");
        this.io = io;
    }

    /*
    * This method is called when the thread is started
    * The parser will parse the request and send the response
     */
    @Override
    public void run() {
        RequestParser parser = new RequestParser(io);
        try {
            parser.parse();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}