package cis5550.webserver.pool;

import cis5550.tools.Logger;
import cis5550.webserver.io.NetworkIOStrategy;
import cis5550.webserver.parser.RequestParser;

import java.io.*;

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