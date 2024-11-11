package org.noova.generic;

import org.noova.tools.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;


public class Worker {
    private static final Logger log = Logger.getLogger(Worker.class);

    protected static int port;
    protected static String storageDir;
    public static String coordinatorAddr;

    public static String id;
    private static final long HEARTBEAT_INTERVAL = 5000;

    private static boolean pingRunning = false;

    public static void startPingThread(String coordinatorAddr, String id, int port) {
        if(pingRunning){
            return;
        }

        log.info("Starting ping thread");

        Thread thread = new Thread(() -> {
            while(true) {
                try {
                    ping(coordinatorAddr, id, port);
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    log.error("Error sleeping", e);
                }
            }
        });

        thread.start();

        pingRunning = true;
    }

    private static void ping(String coordinatorAddr, String id, int port) {
        try {
            URL url = URI.create("http://" + coordinatorAddr+ "/ping?id=" + id + "&port=" + port).toURL();

            // log.info("Pinging: " + url);
            // trigger the http request
            url.getContent();
        } catch (MalformedURLException e) {
            log.error("Error creating ping URL", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("Error pinging", e);
            throw new RuntimeException(e);
        }
    }


    public static String getId(){
        return id;
    }

    public static String getCoordinatorAddr(){
        return coordinatorAddr;
    }

    public static String getStorageDir() {
        return storageDir;
    }

}
