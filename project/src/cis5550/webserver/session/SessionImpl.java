package cis5550.webserver.session;

import cis5550.tools.Logger;
import cis5550.webserver.Session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionImpl implements Session {

    private static final Logger log = Logger.getLogger(SessionImpl.class);

    private final String id;
    private final long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;

    private boolean valid = true;

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public SessionImpl(String id){
        this.id = id;
        creationTime = System.currentTimeMillis();
        lastAccessedTime = creationTime;
        maxActiveInterval = 300 * 1000;
    }
    public SessionImpl(String id, long creationTime, long lastAccessedTime, int maxActiveInterval) {
        this.id = id;
        this.creationTime = creationTime;
        this.lastAccessedTime = lastAccessedTime;
        this.maxActiveInterval = maxActiveInterval;
    }


    public void updateLastAccessedTime() {
        if(isExpired() || isInvalidated()){
            return;
        }
        lastAccessedTime = System.currentTimeMillis();
        log.warn("Session " + id + " update to " + lastAccessedTime);
    }

    private long expirationTime() {
        return lastAccessedTime + maxActiveInterval;
    }

    public boolean isExpired() {
        log.info("Current time: " + System.currentTimeMillis());
        log.info("Session " + id + " expires at " + expirationTime());
        log.info("Max active interval: " + maxActiveInterval);
        return System.currentTimeMillis() > expirationTime();
    }

    public boolean isInvalidated() {
        return !valid;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds * 1000;
        log.info("Session " + id + " max active interval set to " + maxActiveInterval + "ms");
    }

    @Override
    public void invalidate() {
        log.info("Session " + id + " invalidated");
        valid = false;
    }

    @Override
    public Object attribute(String name) {
        return attributes.getOrDefault(name, null);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }
}
