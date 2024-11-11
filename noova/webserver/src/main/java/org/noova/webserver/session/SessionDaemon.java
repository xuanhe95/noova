package org.noova.webserver.session;

import org.noova.tools.Logger;
import org.noova.webserver.host.HostManager;

import java.util.Random;

/**
 * @author Xuanhe Zhang
 *
 * The session daemon is responsible for removing expired sessions
 */
public class SessionDaemon implements Runnable {
    private static final Logger log = Logger.getLogger(SessionDaemon.class);

    private static SessionDaemon INSTANCE;
    private static final int SESSION_CHECK_PERIOD = 5000;
    private static final int SESSION_CHECK_PERIOD_DELTA = 10;

    public static void start(){
        if(INSTANCE == null){
            INSTANCE = new SessionDaemon();
            Thread thread = new Thread(INSTANCE);
            thread.start();
        }
    }

    @Override
    public void run(){
        while(true){
            try{
                Random random = new Random();
                int randomInt = random.nextInt(SESSION_CHECK_PERIOD, SESSION_CHECK_PERIOD + SESSION_CHECK_PERIOD_DELTA);
                log.info("Session Daemon sleeping for " + randomInt + " ms");
                Thread.sleep(randomInt);
                HostManager.removeInvalidAndExpiredSessions();
            } catch (InterruptedException e){
                log.warn("Session Daemon interrupted");
            }
        }
    }
}
